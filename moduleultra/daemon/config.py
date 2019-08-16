
from yaml import load
from os import environ
from os.path import join, isfile

from ..module_ultra_repo import ModuleUltraRepo
from ..module_ultra_config import ModuleUltraConfig


class RepoDaemonConfig:
    """Represent a MU repo to the MU daemon."""

    def __init__(self, **kwargs):
        self.repo_name = kwargs['repo_name']
        self.repo_path = kwargs['repo_path']
        self.pipelines = kwargs['pipelines']

    def get_repo(self):
        """Return the MU repo that this represents."""
        return ModuleUltraRepo(self.repo_path)

    def get_pipeline_list(self):
        """Return a list of (pipe_name, version)."""
        return [(pipe['name'], pipe['version']) for pipe in self.pipelines]

    def get_pipeline_tolerance(self, pipe_name):
        """Return tolerance for the pipeline."""
        for pipe in self.pipelines:
            if pipe['name'] == pipe_name:
                return pipe.get('tolerance', 0)

    def get_pipeline_endpts(self, pipe_name):
        """Return a list of endpts or None."""
        return None

    def get_pipeline_excluded_endpts(self, pipe_name):
        """Return a list of excluded endpts or None."""
        return None


class DaemonConfig:
    """Store config information for the MU daemon."""

    def __init__(self, repos, total_jobs=10, run_local=True, pipeline_configs={}):
        self.repos = repos
        self.total_jobs = int(total_jobs)
        self.run_local = run_local
        self.pipeline_configs = pipeline_configs

    def list_repos(self):
        """Return a list of RepoDaemonConfigs."""
        repo_configs = []
        for repo_name, repo_path, pipelines in self.repos:
            repo_configs.append(RepoDaemonConfig(**{
                'repo_name': repo_name,
                'repo_path': repo_path,
                'pipelines': pipelines,
            }))
        return repo_configs

    def get_pipeline_run_config(self, pipe_name, pipe_version):
        """Return a filepath for the config to be used or None."""
        return None

    @classmethod
    def get_daemon_config_filename(ctype):
        try:
            return environ['MODULE_ULTRA_DAEMON_CONFIG']
        except KeyError:
            config_dir = ModuleUltraConfig.getConfigDir()
            config_filename = join(config_dir, 'daemon_config.yaml')
            if isfile(config_filename):
                return config_filename
        assert False, "No daemon config found"

    @classmethod
    def load_from_yaml(ctype, yaml_filename=None):
        yaml_filename = yaml_filename if yaml_filename else ctype.get_daemon_config_filename()
        raw_config = load(open(yaml_filename))
        raw_repos = raw_config['repos']
        repo_list = [
            (raw_repo['name'], raw_repo['path'], raw_repo['pipelines'])
            for raw_repo in raw_repos
        ]
        return DaemonConfig(
            repo_list,
            total_jobs=raw_config.get('num_jobs', 10),
            run_local=raw_config.get('run_on_cluster', True),
            pipeline_configs=raw_config.get('pipeline_configs', {})
        )
