#!/usr/bin/env python

# =============================================================================
# Initialization
# =============================================================================

from active_worker.task import task
from task_types import TaskTypes as tt
import unicore_client
import os


def load_local_file(name):
    """ loads local file and returns contents as string """
    file_path = os.path.join(os.path.dirname(__file__), name)
    with open(file_path) as f:
        return f.read()


@task
def cch_vista_submit_task(inputdata_spinnaker, inputdata_nest, run_script,
                          collect_script, num_tasks):
    '''
        Task Manifest Version: 1
        Full Name: cch_vista_submit_task
        Caption: cch_vista_submit_task
        Author: Elephant-Developers
        Description: |
            This task submits a script to an HPC, calculates the pairwise
            correlation and stores an p-value significance matrix, which can be 
            read in by the `vista` visualization framework.
        Categories:
            - FDAT
        Compatible_queues: ['cscs_viz']
        Accepts:
            inputdata_spinnaker:
                type: application/unknown
                description: Input file that contains spiking data from a
                    HDF5 file generated from spinnaker simulation.
            inputdata_nest:
                type: application/unknown
                description: |
                    Input file that contains spiking data from a
                    HDF5 file from nest simulation.
            run_script:
                type: text/x-python
                description: Script which will be executed on an HPC.
            collect_script:
                type: text/x-python
                description: Script which will be executed on an HPC.
            num_tasks:
                type: long
                description: Number of tasks which will be run on the HPC.
        Returns:
            res: application/unknown
    '''
    # Get paths
    spinnaker_data_path = cch_vista_submit_task.task.uri.get_file(
        inputdata_spinnaker)
    nest_data_path = cch_vista_submit_task.task.uri.get_file(inputdata_nest)
    run_script_path = cch_vista_submit_task.task.uri.get_file(run_script)
    collect_script_path = cch_vista_submit_task.task.uri.get_file(collect_script)
    # Load h5 wrapper
    wrapper_path = 'wrapper.py'

    # Preparing for unicore submission
    code = {'To': 'input.py',
            'Data': load_local_file('{0}'.format(run_script_path))}
    collect_script = {'To': 'collect.py',
                      'Data': load_local_file('{0}'.format(collect_script_path))}
    h5_script = {'To': '{0}'.format(wrapper_path),
                 'Data': load_local_file(
                     '{0}'.format(wrapper_path))}
    spinnaker_data = {'To': 'spinnaker_data.h5',
                      'Data': load_local_file('{0}'.format(spinnaker_data_path))}
    nest_data = {'To': 'nest_data.h5',
                 'Data': load_local_file('{0}'.format(nest_data_path))}
    inputs = [code, collect_script, h5_script, spinnaker_data, nest_data]

    # Get token
    oauth_token = cch_vista_submit_task.task.uri.get_oauth_token()
    auth = unicore_client.get_oidc_auth(oauth_token)

    # Unicore parameter
    job = dict()
    job['ApplicationName'] = 'Elephant'
    job['Environment'] = {'INPUT': 'input.py',
                          'spinnaker_data': 'spinnaker_data.h5',
                          'nest_data': 'nest_data.h5',
                          'NUM_TASKS': str(num_tasks),
                          }
    job['Resources'] = {'ArraySize': str(num_tasks), 'Runtime': '3h'}
    job['Execution environment'] = {'Name': 'Elephant',
                                    'PostCommands': ['COLLECT']}

    # (hackish) export to dCache for visualisation
    results = ['viz_output_nest.h5', 'viz_output_nest.pkl',
               'viz_output_spinnaker.h5', 'viz_output_spinnaker.pkl']
    exports = []
    for result in results:
        exports.append({'From': 'results/' + result,
                        'To': 'https://jade01.zam.kfa-juelich.de:2880/HBP/summit15/nest-elephant/' + result,
                        'Credentials': {'Username': 'jbiddiscombe',
                                        'Password': 'Aithahs8'},
                        'FailOnError': 'false',
                        })
    job['Exports'] = exports

    # Submission
    base_url = unicore_client.get_sites()['JURECA']['url']
    job_url = unicore_client.submit(os.path.join(base_url, 'jobs'), job, auth,
                                    inputs)
    print "Submitting to {0}".format(job_url)
    unicore_client.wait_for_completion(job_url, auth,
                                       refresh_function=cch_vista_submit_task.task.uri.get_oauth_token)

    # Get results and store them to task-local storage
    # create bundle & export bundle
    workdir = unicore_client.get_working_directory(job_url, auth)
    fn = ''
    for filename in results:
        content = unicore_client.get_file_content(
            workdir + "/files/results/" + filename, auth)
        if filename.endswith(".h5"):
            with open(filename, "w") as local_file:
                local_file.write(content)
            fn = filename

    return cch_vista_submit_task.task.uri.save_file(mime_type='application/unknown',
                                                    src_path=fn,
                                                    dst_path=os.path.join(
                                                        'contents', fn))


if __name__ == '__main__':
    inputdata_spinnaker = tt.URI('application/unknown', 'spikes_L5E.h5')
    inputdata_nest = tt.URI('application/unknown', 'spikes_L5E.h5')
    script_run = tt.URI('text/x-python', 'cch_cluster_spinnest.py')
    script_collect = tt.URI('text/x-python', 'cch_collect_spinnest.py')
    num_task = 100
    cch_vista_submit_task(inputdata_spinnaker, inputdata_nest, script_run,
                          script_collect, num_task)
