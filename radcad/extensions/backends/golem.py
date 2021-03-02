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

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))


script_dir = pathlib.Path(__file__).resolve().parent
pickle_dir = script_dir / 'pickles'


class ExecutorGolemRemote(Executor):

    def execute_runs(self):
        print("Golem Remote")
#         result = [
#             core._single_run_wrapper((config, self.engine.raise_exceptions))
#             for config in self.engine.run_generator
#         ]
#         return result

        # result = [
        #     core._single_run_wrapper((config, self.engine.raise_exceptions))
        #     for config in self.engine.run_generator
        # ]
        # return result


class ExecutorGolem(Executor):

    async def main(self):
        package = await vm.repo(
            image_hash="77e806cde851a8e0315892d341903492e4e69f29359232fb25e5a31d",
            min_mem_gib=0.5,
            min_storage_gib=2.0,
        )

        async def worker(ctx: WorkContext, tasks):
            async for task in tasks:

                input_file = str(task.data)

                ctx.send_file(input_file, "/golem/resource/radcad.prep")
                ctx.run("/usr/bin/sh",  "-c",  "mv /golem/resource/radcad.prep /golem/output/radcad.output")
#
                output_file = f"{input_file}.out"
                print(output_file)
                ctx.download_file(
                    "/golem/output/radcad.output", output_file)
#
                try:
                    # Set timeout for executing the script on the provider. Two minutes is plenty
                    # of time for computing a single frame, for other tasks it may be not enough.
                    # If the timeout is exceeded, this worker instance will be shut down and all
                    # remaining tasks, including the current one, will be computed by other providers.
                    yield ctx.commit(timeout=timedelta(seconds=120))
                    # TODO: Check if job results are valid
                    # and reject by: task.reject_task(reason = 'invalid file')
                    task.accept_result(result=output_file)
                except BatchTimeoutError:
                    print(
                        f"{TEXT_COLOR_RED}"
                        f"Task {task} timed out on {ctx.provider_name}, time: {task.running_time}"
                        f"{TEXT_COLOR_DEFAULT}"
                    )
                    raise

        # Worst-case overhead, in minutes, for initialization (negotiation, file transfer etc.)
        # TODO: make this dynamic, e.g. depending on the size of files to transfer
        init_overhead = 3
        # Providers will not accept work if the timeout is outside of the [5 min, 30min] range.
        # We increase the lower bound to 6 min to account for the time needed for our demand to
        # reach the providers.
        min_timeout, max_timeout = 6, 30

        timeout = timedelta(minutes=max(min(
            init_overhead + self.engine.golem_timeout, max_timeout),
            min_timeout))

        # By passing `event_consumer=log_summary()` we enable summary logging.
        # See the documentation of the `yapapi.log` module on how to set
        # the level of detail and format of the logged information.
        print("Max workers", self.engine.golem_nodes)
        print("Budget :", self.engine.golem_budget)

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
        except KeyboardInterrupt:
            print(
                f"{TEXT_COLOR_YELLOW}"
                "Shutting down gracefully, please wait a short while "
                "or press Ctrl+C to exit immediately..."
                f"{TEXT_COLOR_DEFAULT}"
            )
            task.cancel()
            try:
                loop.run_until_complete(task)
                print(
                    f"{TEXT_COLOR_YELLOW}Shutdown completed, thank you for waiting!{TEXT_COLOR_DEFAULT}"
                )
            except (asyncio.CancelledError, KeyboardInterrupt):
                pass
