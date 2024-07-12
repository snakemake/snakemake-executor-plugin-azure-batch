__author__ = "Johannes Köster, Jake VanCampen, Andreas Wilm"
__copyright__ = "Copyright 2023, Snakemake community"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"


from snakemake_interface_executor_plugins.settings import CommonSettings

from .executor import Executor as Executor  # noqa

# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Define whether your executor plugin implies that there is no shared
    # filesystem (True) or not (False).
    # This is e.g. the case for cloud execution.
    implies_no_shared_fs=True,
    job_deploy_sources=True,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=False,
    auto_deploy_default_storage_provider=True,
)
