
from radcad.extensions.backends.golem import golem_remote_loader, golem_remote_pickler
from radcad import Experiment
from radcad.engine import Engine
from radcad.backends import Backend
import numpy as np
import json

with open('/golem/work/params.json', 'r') as params_file:
    data = params_file.read()


params = json.loads(data)

experiment = Experiment()

experiment.engine = Engine(
    backend=Backend(params['backend']),
    pre_gen_runs=golem_remote_loader(),
    raise_exceptions=params["raise_exceptions"],
    process_exceptions=False,
    deepcopy=params["deepcopy"],
    drop_substeps=params["drop_substeps"]
)


golem_remote_pickler(experiment.run())
