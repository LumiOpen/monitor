import os 
import collections
import slurm

from slurmmonitor.config import job_config, free_bytes_config, free_inodes_config, users


def get_free_bytes(path):
    stats = os.statvfs(path)
    return stats.f_bfree * stats.f_frsize

def get_free_inodes(path):
    stats = os.statvfs(path)
    return stats.f_ffree

class ClusterDataSnapshot:
    def __init__(self):
        jobs, jobs_running_count, jobs_stalled = self._get_job_status(job_config, users)
        self.jobs = jobs
        self.jobs_running_count = jobs_running_count
        self.jobs_stalled = jobs_stalled

        self.free_inodes = {path: get_free_inodes(path) for path in free_inodes_config}
        self.free_bytes = {path: get_free_bytes(path) for path in free_bytes_config}

        self.queue_days = {"standard-g": slurm.util.get_queue_days("standard-g")}
    
    def _get_job_status(self, job_config, users):
        jobs = {}
        jobs_running_count = collections.defaultdict(int)
        jobs_stalled = {}

        configs = {job.name: job for job in job_config}

        for job in slurm.util.get_job_state(users):
            # check for only the job names we're interested in.
            if job.name not in configs.keys():
                continue
    
            # names are not guaranteed unique, so let's look for duplicates here that might throw our monitoring off.
            if job.running:
                jobs_running_count[job.name] += 1
    
            seen_job = jobs.get(job.name, None)
            if seen_job is None:
                jobs[job.name] = job
            elif job.running:
                # running jobs should take precedence in status reporting over
                # pending jobs.
                jobs[job.name] = job

            # check if job is stalled (only running jobs can be stalled!)
            if job.running and configs[job.name].stalled():
                jobs_stalled[job.name] = True

        return jobs, jobs_running_count, jobs_stalled


    def _get_job_stalls(self, job_config):
        for job in job_config:
            current_job = jobs.get(job.name, None)
            if current_job is not None:
                if current_job.running and job.stalled():
                    messages.append(f"{job.name} looks stalled: {job.logfile}")


    
