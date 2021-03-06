import pytest
import pandas as pd

from radcad import Model, Simulation, Experiment
from tests.test_cases import benchmark_model

states = benchmark_model.states
state_update_blocks = benchmark_model.state_update_blocks
params = benchmark_model.params
TIMESTEPS = 100_000
RUNS = 3

model = Model(initial_state=states, state_update_blocks=state_update_blocks, params=params)
simulation_radcad = Simulation(model=model, timesteps=TIMESTEPS, runs=RUNS)
experiment = Experiment([simulation_radcad, simulation_radcad, simulation_radcad])

def test_benchmark_radcad(benchmark):
    benchmark.pedantic(radcad_simulation, iterations=1, rounds=3)

def radcad_simulation():
    data_radcad = experiment.run()
