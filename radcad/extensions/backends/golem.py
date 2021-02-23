try:
    print("Hi from GOlem")
    # import ray
except ImportError:
    _has_extension = False
else:
    _has_extension = True


if not _has_extension:
    raise Exception("Optional extension dependency Ray not installed")


from radcad.backends import Executor
import radcad.core as core


class ExecutorGolem(Executor):
    #    def _proxy_single_run(args):
    #         return core._single_run_wrapper(args)

    def execute_runs(self):
        print("Golem local")
        result = [
            core._single_run_wrapper((config, self.engine.raise_exceptions))
            for config in self.engine.run_generator
        ]
        return result
#         # ray.init(num_cpus=self.engine.processes, ignore_reinit_error=True)
#         futures = [
#             ExecutorRay._proxy_single_run.remote((config, self.engine.raise_exceptions))
#             for config in self.engine.run_generator
#         ]
#         return "bye"  # ray.get(futures)


class ExecutorGolemRemote(Executor):
    #     def _proxy_single_run(args):
    #         return core._single_run_wrapper(args)
    #

    def execute_runs(self):
        print("Golem Remote")
        result = [
            core._single_run_wrapper((config, self.engine.raise_exceptions))
            for config in self.engine.run_generator
        ]
        return result
#         print(
#             "Using Ray remote backend, please ensure you've initialized Ray using ray.init(address=***, ...)"
#         )
#         futures = [
#             # ExecutorRayRemote._proxy_single_run.remote((config, self.engine.raise_exceptions))
#             for config in self.engine.run_generator
#         ]
#         return "hi"# ray.get(futures)
