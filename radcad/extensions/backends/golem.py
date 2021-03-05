try:
    from yapapi import Executor as g_Executor
    from yapapi import (
        NoPaymentAccountError,
        Task,
        __version__ as yapapi_version,
        WorkContext,
        windows_event_loop_fix,
    )
    from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa
    from yapapi.package import vm
    from yapapi.rest.activity import BatchTimeoutError
except ImportError:
    raise Exception("Optional extension dependency yapapi not installed")


from radcad.backends import Executor
import radcad.core as core

import pathlib
from datetime import datetime, timedelta
import asyncio
import nest_asyncio
import sys
import os
import dill

from radcad.utils import (
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_RED,
    TEXT_COLOR_YELLOW,
)

script_dir = pathlib.Path(__file__).resolve().parent
pickle_dir = script_dir / 'golem/pickles'

remote_pickle_in = pathlib.Path('/golem/resource/radcad.prep')
remote_pickle_out = pathlib.Path('/golem/output/radcad.output')

golem_exec_local = script_dir / 'golem/radcad_remote_agent.py'
golem_exec_remote = '/golem/work/radcad_remote_agent.py'


output_files = []


def golem_remote_loader():
    configs = dill.load(remote_pickle_in.open('rb'))

    return configs


def golem_remote_pickler(result):
    dill_out = remote_pickle_out.open("wb")
    dill.dump(result, dill_out)
    dill_out.close()


class ExecutorGolem(Executor):

    async def main(self):
        package = await vm.repo(
            image_hash="086376ab1d5b7e9c2ae4026d2bea5ea6e612c31a6fcc24cc347c726a",
            min_mem_gib=self.engine.golem_mem[0],
            min_storage_gib=self.engine.golem_storage[0]
        )

        async def worker(ctx: WorkContext, tasks):
            async for task in tasks:

                input_file = str(task.data)

                ctx.send_file(input_file, remote_pickle_in)
                ctx.send_file(golem_exec_local, golem_exec_remote)

                ctx.send_json(
                    "/golem/work/params.json",
                    {
                        "backend": self.engine.golem_backend[0],
                        "process_exceptions": self.engine.process_exceptions,
                        "raise_exceptions": self.engine.raise_exceptions,
                        "deepcopy": self.engine.deepcopy,
                        "drop_substeps": self.engine.drop_substeps
                    },
                )

                ctx.run("/usr/bin/sh", "-c", "python3 /golem/work/radcad_remote_agent.py")

                output_file = pathlib.Path(f"{input_file}.procd")
                output_files.append(output_file)
                ctx.download_file(remote_pickle_out, output_file)

                try:
                    # If the timeout is exceeded, this worker instance will
                    # be shut down and all remaining tasks, including the
                    # current one, will be computed by other providers.
                    yield ctx.commit(timeout=timedelta(seconds=self.engine.golem_timeout * 60))

                    # TODO: Check if job results are valid
                    # and reject by: task.reject_task(reason = 'invalid file')
                    task.accept_result(result=output_file)
#                    task.accept_result(result=log_file)
                except BatchTimeoutError:
                    print(
                        f"{TEXT_COLOR_RED}"
                        f"Task {task} timed out on {ctx.provider_name}, time: {task.running_time}"
                        f"{TEXT_COLOR_DEFAULT}"
                    )
                    raise

        # Worst-case overhead, in minutes, for initialization
        # (negotiation, file transfer etc.)
        init_overhead = 3
        # Providers will not accept work if the timeout is outside of the
        # [5 min, 30min] range.  We increase the lower bound to 6 min to
        # account for the time needed for our demand to reach the providers.
        min_timeout, max_timeout = 6, 30

        timeout = timedelta(minutes=max(min(
            init_overhead + self.engine.golem_timeout, max_timeout),
            min_timeout))

        # By passing `event_consumer=log_summary()` we enable summary logging.
        # See the documentation of the `yapapi.log` module on how to set
        # the level of detail and format of the logged information.


        async with g_Executor(
            package=package,
            max_workers=self.engine.golem_nodes,
            budget=self.engine.golem_budget,
            timeout=timeout,
            subnet_tag=self.engine.golem_subnet_tag,
            driver=self.engine.golem_driver,
            network=self.engine.golem_network,
            event_consumer=log_summary(log_event_repr),
        ) as executor:

            sys.stderr.write(
                f"yapapi version: {TEXT_COLOR_YELLOW}{yapapi_version}{TEXT_COLOR_DEFAULT}\n"
                f"Using subnet: {TEXT_COLOR_YELLOW}{self.engine.golem_subnet_tag}{TEXT_COLOR_DEFAULT}, "
                f"payment driver: {TEXT_COLOR_YELLOW}{executor.driver}{TEXT_COLOR_DEFAULT}, "
                f"and network: {TEXT_COLOR_YELLOW}{executor.network}{TEXT_COLOR_DEFAULT}\n"
            )

            num_tasks = 0
            start_time = datetime.now()

            async for task in executor.submit(worker, [Task(data=pickle) for pickle in self.pickles]):
                num_tasks += 1
                print(
                    f"{TEXT_COLOR_CYAN}"
                    f"Task computed: {task}, result: {task.result}, time: {task.running_time}"
                    f"{TEXT_COLOR_DEFAULT}"
                )

            print(
                f"{TEXT_COLOR_CYAN}"
                f"{num_tasks} tasks computed, total time: {datetime.now() - start_time}"
                f"{TEXT_COLOR_DEFAULT}"
            )

    def execute_runs(self):
        os.environ['YAGNA_APPKEY'] = self.engine.golem_yagna_key

        config_list = list(self.engine.run_generator)

        bundles = dict()
        for bundle in range(self.engine.golem_bundles):
            bundles[bundle] = config_list[bundle::self.engine.golem_bundles]

        # Create pickles for transfer to network later on
        if not pickle_dir.exists():
            pickle_dir.mkdir()
        else:
            [x.unlink() for x in pickle_dir.iterdir()]

        self.pickles = []
        for bundle in bundles:
            filename = pickle_dir / ("bundle_"+str(bundle)+".pickle")
            self.pickles.append(filename)

            dill_out = filename.open("wb")
            dill.dump(bundles[bundle], dill_out)
            dill_out.close()

        # This is only required when running on Windows with Python prior to 3.8:
        windows_event_loop_fix()

        enable_default_logger(
            log_file=self.engine.golem_log_file,
            debug_activity_api=self.engine.golem_debug_activity,
            debug_market_api=self.engine.golem_debug_market,
            debug_payment_api=self.engine.golem_debug_payment,
        )

        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        task = loop.create_task(
            ExecutorGolem.main(self)
        )

        try:
            loop.run_until_complete(task)
        except NoPaymentAccountError as e:
            handbook_url = (
                "https://handbook.golem.network/requestor-tutorials/"
                "flash-tutorial-of-requestor-development"
            )
            print(
                f"{TEXT_COLOR_RED}"
                f"No payment account initialized for driver `{e.required_driver}` "
                f"and network `{e.required_network}`.\n\n"
                f"See {handbook_url} on how to initialize payment accounts for a requestor node."
                f"{TEXT_COLOR_DEFAULT}"
            )
            [x.unlink() for x in pickle_dir.iterdir()]
        except KeyboardInterrupt:
            print(
                f"{TEXT_COLOR_YELLOW}"
                "Shutting down gracefully, please wait a short while "
                "or press Ctrl+C to exit immediately..."
                f"{TEXT_COLOR_DEFAULT}"
            )
            task.cancel()
            [x.unlink() for x in pickle_dir.iterdir()]
            try:
                loop.run_until_complete(task)
                print(
                    f"{TEXT_COLOR_YELLOW}Shutdown completed, thank you for waiting!{TEXT_COLOR_DEFAULT}"
                )
            except (asyncio.CancelledError, KeyboardInterrupt):
                pass
        else:
            results = []
            for result_file in output_files:
                results.extend(dill.load(result_file.open('rb')))
            [x.unlink() for x in pickle_dir.iterdir()]
            return results
