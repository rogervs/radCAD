import radcad.core as core
import radcad.wrappers as wrappers
from radcad.backends import Backend
from radcad.utils import flatten, extract_exceptions

import multiprocessing
import dill
import copy


# Get machine CPU count
cpu_count = multiprocessing.cpu_count() - 1 or 1


class Engine:
    def __init__(self, **kwargs):
        self.experiment = None
        self.processes = kwargs.pop("processes", cpu_count)
        self.backend = kwargs.pop("backend", Backend.DEFAULT)
        self.raise_exceptions = kwargs.pop("raise_exceptions", True)
        self.process_exceptions = kwargs.pop("process_exceptions", True)
        self.deepcopy = kwargs.pop("deepcopy", True)
        self.drop_substeps = kwargs.pop("drop_substeps", False)
        self.pre_gen_runs = kwargs.pop("pre_gen_runs", None)
        self.run_generator = iter(())



        # Check if GOLEM backend is selcted and parse values as needed
        if self.backend == Backend.GOLEM:
            try:
                golem_conf = kwargs.pop("golem_conf")
            except KeyError:
                raise KeyError("golem_conf dictionary is required when the GOLEM backend is selected")
            else:
                self.golem_nodes = golem_conf.pop('NODES', 3)
                self.golem_backend = golem_conf.pop('REMOTE_BACKEND', Backend.SINGLE_PROCESS).value,
                self.golem_mem = golem_conf.pop('MEMORY', 0.5),
                self.golem_storage = golem_conf.pop('STORAGE', 2.0),
                self.golem_bundles = golem_conf.pop('BUNDLES', self.golem_nodes)
                self.golem_budget = golem_conf.pop('BUDGET', 10)
                self.golem_subnet_tag = golem_conf.pop('SUBNET_TAG', 'community.4')
                self.golem_driver = golem_conf.pop('PAYMENT_DRIVER', 'zksync')
                self.golem_network = golem_conf.pop('NETWORK', 'rinkeby')
                self.golem_timeout = golem_conf.pop('TIMEOUT', 2)
                self.golem_log_file = golem_conf.pop('LOG_FILE', 'radcad_golem.log')
                self.golem_debug_activity = golem_conf.pop('DEBUG_ACTIVITY', False)
                self.golem_debug_market = golem_conf.pop('DEBUG_MARKET', False)
                self.golem_debug_payment = golem_conf.pop('DEBUG_PAYMENT', False)
                try:
                    self.golem_yagna_key = golem_conf.pop('YAGNA_KEY')
                except KeyError:
                    raise Exception("YAGNA_KEY missing from golem_conf dictionary")

        if kwargs:
            raise Exception(f"Invalid Engine option in {kwargs}")

    def _run(self, experiment=None, **kwargs):
        if not experiment:
            raise Exception("Experiment required as argument")
        self.experiment=experiment

        if kwargs:
            raise Exception(f"Invalid Engine option in {kwargs}")

        simulations=experiment.simulations
        if not isinstance(self.backend, Backend):
            raise Exception(
                f"Execution backend must be one of {Backend.list()}")
        configs=[
            (
                sim.model.initial_state,
                sim.model.state_update_blocks,
                sim.model.params,
                sim.timesteps,
                sim.runs,
            )
            for sim in simulations
        ]

        result = []

        self.experiment._before_experiment(experiment=self.experiment)

        if self.pre_gen_runs:
            self.run_generator = iter(self.pre_gen_runs)
        else:
            self.run_generator = self._run_stream(configs)

        # Select backend executor
        if self.backend in [Backend.RAY, Backend.RAY_REMOTE]:
            if self.backend == Backend.RAY_REMOTE:
                from radcad.extensions.backends.ray import ExecutorRayRemote as Executor
            else:
                from radcad.extensions.backends.ray import ExecutorRay as Executor
        elif self.backend in [Backend.PATHOS, Backend.DEFAULT]:
            from radcad.backends.pathos import ExecutorPathos as Executor
        elif self.backend in [Backend.MULTIPROCESSING]:
            from radcad.backends.multiprocessing import ExecutorMultiprocessing as Executor
        elif self.backend in [Backend.SINGLE_PROCESS]:
            from radcad.backends.single_process import ExecutorSingleProcess as Executor
        elif self.backend in [Backend.GOLEM]:
            from radcad.extensions.backends.golem import ExecutorGolem as Executor
        else:
            raise Exception(
                f"Execution backend must be one of {Backend._member_names_}, not {self.backend}")

        result=Executor(self).execute_runs()

        if self.process_exceptions:
            self.experiment.results, self.experiment.exceptions = extract_exceptions(result)
            self.experiment._after_experiment(experiment=self.experiment)
            return self.experiment.results
        else:
            return result

    def _get_simulation_from_config(config):
        states, state_update_blocks, params, timesteps, runs=config
        model=wrappers.Model(
            initial_state=states, state_update_blocks=state_update_blocks, params=params
        )
        return wrappers.Simulation(model=model, timesteps=timesteps, runs=runs)

    def _run_stream(self, configs):
        simulations=[Engine._get_simulation_from_config(
            config) for config in configs]

        for simulation_index, simulation in enumerate(simulations):
            simulation.index=simulation_index

            timesteps=simulation.timesteps
            runs=simulation.runs
            initial_state=simulation.model.initial_state
            state_update_blocks=simulation.model.state_update_blocks
            params=simulation.model.params
            param_sweep=core.generate_parameter_sweep(params)

            self.experiment._before_simulation(
                simulation=simulation
            )

            # NOTE Hook allows mutation of RunArgs
            for run_index in range(0, runs):
                if param_sweep:
                    context=wrappers.Context(
                        simulation_index,
                        run_index,
                        None,
                        timesteps,
                        initial_state,
                        params
                    )
                    self.experiment._before_run(context=context)
                    for subset_index, param_set in enumerate(param_sweep):
                        context=wrappers.Context(
                            simulation_index,
                            run_index,
                            subset_index,
                            timesteps,
                            initial_state,
                            params
                        )
                        self.experiment._before_subset(context=context)
                        yield wrappers.RunArgs(
                            simulation_index,
                            timesteps,
                            run_index,
                            subset_index,
                            copy.deepcopy(initial_state),
                            state_update_blocks,
                            copy.deepcopy(param_set),
                            self.deepcopy,
                            self.drop_substeps,
                        )
                        self.experiment._after_subset(context=context)
                    self.experiment._before_run(context=context)
                else:
                    context=wrappers.Context(
                        simulation_index,
                        run_index,
                        0,
                        timesteps,
                        initial_state,
                        params
                    )
                    self.experiment._before_run(context=context)
                    yield wrappers.RunArgs(
                        simulation_index,
                        timesteps,
                        run_index,
                        0,
                        copy.deepcopy(initial_state),
                        state_update_blocks,
                        copy.deepcopy(params),
                        self.deepcopy,
                        self.drop_substeps,
                    )
                    self.experiment._after_run(context=context)

            self.experiment._after_simulation(
                simulation=simulation
            )
