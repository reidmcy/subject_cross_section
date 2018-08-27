from parsl.config import Config
from libsubmit.providers import LocalProvider
from parsl.executors.ipp import IPyParallelExecutor
from libsubmit.providers import SlurmProvider
from libsubmit.channels import LocalChannel

localNode = Config(
    executors=[
        IPyParallelExecutor(
                provider=LocalProvider(
                    init_blocks=2,
                    min_blocks=1,
                    max_blocks=4,
                ),
                label='ipp',
            ),
    ],
    app_cache=False,
)

rccNodeExclusive = Config(
    executors=[
        IPyParallelExecutor(
            label='midway_ipp',
            provider=SlurmProvider(
                'broadwl',
                channel=LocalChannel(),
                #launcher=SrunLauncher(),
                init_blocks=1, # 1
                min_blocks=0, # 0
                max_blocks=3, # 10
                nodes_per_block=1, # 4
                tasks_per_node=28, # 12
                walltime="00:30:00",
                overrides="#SBATCH --mem-per-cpu=2000\n#SBATCH --account=pi-jevans\n#SBATCH --exclusive\nsource /project2/jevans/virt_sbatch/bin/activate\nmodule load java/1.8\nulimit -u 10000\nrm -rf `ls -la /tmp | grep 'reidmcy' | awk ' { print $9 } '`"
                #WARNING: Watch out for the rm /tmp, only use on exclusive
            ),
        ),
    ],
    app_cache=False,
)

rccNodeSmall = Config(
    executors=[
        IPyParallelExecutor(
            label='midway_ipp',
            provider=SlurmProvider(
                'broadwl',
                channel=LocalChannel(),
                #launcher=SrunLauncher(),
                init_blocks=1, # 1
                min_blocks=0, # 0
                max_blocks=1, # 10
                nodes_per_block=1, # 4
                tasks_per_node=10, # 12
                walltime="00:30:00",
                overrides="#SBATCH --mem-per-cpu=2000\n#SBATCH --account=pi-jevans\n\nsource /project2/jevans/virt_sbatch/bin/activate\nmodule load java/1.8\nulimit -u 10000"
            ),
        ),
    ],
    app_cache=False,
)
