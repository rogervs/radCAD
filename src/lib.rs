use pyo3::exceptions::TypeError;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyString, PyDict, PyList, PyTuple};
use pyo3::wrap_pyfunction;
use std::convert::TryFrom;
use std::collections::HashMap;


#[pymodule]
fn rad_cad(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Model>()?;
    m.add_class::<Simulation>()?;
    m.add_wrapped(wrap_pyfunction!(run))?;

    Ok(())
}

#[pyclass]
#[derive(Debug, Clone)]
struct Model {
    initial_state: PyObject,
    psubs: Vec<HashMap<String, HashMap<String, PyObject>>>,
    params: PyObject
}

#[pymethods]
impl Model {
    #[new]
    fn new(initial_state: PyObject, psubs: Vec<HashMap<String, HashMap<String, PyObject>>>, params: PyObject) -> Self {
        Model { initial_state, psubs, params }
    }
}

#[pyclass]
#[derive(Debug, Clone)]
struct Simulation {
    model: Model,
    timesteps: usize,
    runs: usize
}

#[pymethods]
impl Simulation {
    #[new]
    #[args(timesteps = "100", runs = "1")]
    fn new(timesteps: usize, runs: usize, model: Model) -> Self {
        Simulation { timesteps, runs, model }
    }
}

#[pyfunction]
fn run(
    simulations: &PyList
) -> PyResult<PyObject> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    let result: &PyList = PyList::empty(py);

    for (simulation_index, simulation_) in simulations.iter().enumerate() {
        let simulation: &Simulation = &simulation_.extract::<Simulation>()?;
        let timesteps = simulation.timesteps;
        let runs = simulation.runs;
        let initial_state: &PyDict = simulation.model.initial_state.extract(py)?;
        let psubs: &Vec<HashMap<String, HashMap<String, PyObject>>> = &simulation.model.psubs;
        let params: &PyDict = simulation.model.params.extract(py)?;

        let param_sweep = PyList::empty(py);
        let mut max_len = 0;
        
        for value in params.values() {
            if value.len()? > max_len {
                max_len = value.len()?;
            }
        }

        for sweep_index in 0..max_len {
            let param_set = PyDict::new(py);
            for (key, value) in params {
                let param = if sweep_index < value.len()? {
                    value.get_item(sweep_index)?
                } else {
                    value.get_item(value.len()? - 1)?
                };
                param_set.set_item(key, param)?;
            }
            param_sweep.append(param_set)?;
        }

        for run in 0..runs {
            if param_sweep.len() > 0 {
                for (subset, param_set) in param_sweep.iter().enumerate() {
                    result
                        .call_method(
                            "extend",
                            (single_run(
                                py,
                                simulation_index,
                                timesteps,
                                run,
                                subset,
                                initial_state,
                                psubs,
                                param_set.extract()?,
                            )?,),
                            None,
                        )
                        .unwrap();
                }
            } else {
                result
                    .call_method(
                        "extend",
                        (single_run(py, simulation_index, timesteps, run, 0, initial_state, psubs, params)?,),
                        None,
                    )
                    .unwrap();
            }
        }
    }
    Ok(result.into())
}

fn single_run(
    py: Python,
    simulation: usize,
    timesteps: usize,
    run: usize,
    subset: usize,
    initial_state: &PyDict,
    psubs: &Vec<HashMap<String, HashMap<String, PyObject>>>,
    params: &PyDict,
) -> PyResult<PyObject> {
    let result: &PyList = PyList::empty(py);
    initial_state.set_item("simulation", simulation).unwrap();
    initial_state.set_item("subset", subset).unwrap();
    initial_state.set_item("run", run + 1).unwrap();
    initial_state.set_item("substep", 0).unwrap();
    initial_state.set_item("timestep", 0).unwrap();
    result.append(initial_state.copy()?).unwrap();
    for timestep in 0..timesteps {
        let previous_state: &PyDict = result
            .get_item(isize::try_from(result.len() - 1).expect("Failed to fetch previous state"))
            .cast_as::<PyDict>()?
            .copy()?;
        previous_state.set_item("simulation", simulation).unwrap();
        previous_state.set_item("subset", subset).unwrap();
        previous_state.set_item("run", run + 1).unwrap();
        previous_state.set_item("timestep", timestep + 1).unwrap();
        let substeps: &PyList = PyList::empty(py);
        for (substep, psub) in psubs.into_iter().enumerate() {
            let substate: &PyDict = match substep {
                0 => previous_state.cast_as::<PyDict>()?.copy()?,
                _ => substeps
                    .get_item(isize::try_from(substep - 1).expect("Failed to fetch substate"))
                    .cast_as::<PyDict>()?
                    .copy()?,
            };
            substate.set_item("substep", substep + 1).unwrap();
            for (state, function) in psub
                .get("variables")
                .expect("Get variables failed")
            {
                let state_update: &PyTuple = match function.cast_as::<PyAny>(py)?.is_callable() {
                    true => {
                        function
                        .call(py,
                            (
                                params,
                                substep,
                                result,
                                substate,
                                reduce_signals(
                                    py,
                                    params,
                                    substep,
                                    result,
                                    substate,
                                    psub,
                                )
                                .into_py_dict(py)
                                .clone(),
                            ),
                            None,
                        )?
                        .extract(py)?
                    }
                    false => {
                        return Err(PyErr::new::<TypeError, _>(
                            "State update function is not callable",
                        ));
                    }
                };
                let state_key = state_update.get_item(0);
                let state_value = state_update.get_item(1);
                match PyString::new(py, state) == state_key.str()? {
                    true => substate.set_item(state_key, state_value).unwrap(),
                    false => {
                        return Err(PyErr::new::<TypeError, _>(
                            "PSUB state key doesn't match function state key",
                        ))
                    }
                }
            }
            substeps
                .insert(
                    isize::try_from(substep).expect("Failed to insert substep"),
                    substate,
                )
                .unwrap();
        }
        result.call_method("extend", (substeps,), None).unwrap();
    }
    Ok(result.into())
}

fn reduce_signals(
    py: Python,
    params: &PyDict,
    substep: usize,
    result: &PyList,
    substate: &PyDict,
    psub: &HashMap<String, HashMap<String, PyObject>>,
) -> &PyDict {
    let policy_results = PyList::empty(py);
    for (_var, function) in psub
        .get("policies")
        .expect("Get policies failed")
    {
        policy_results.append(
            function
                .call(py, (params, substep, result, substate.copy().unwrap()), None)
                .unwrap()
                .extract(py)
                .unwrap(),
        );
    }

    match policy_results.len() {
        0 => PyDict::new(py),
        1 => policy_results.get_item(isize::try_from(policy_results.len() - 1).expect("Failed to fetch policy results")).extract::<&PyDict>().unwrap().clone(),
        _ => policy_results
            .iter()
            .fold(PyDict::new(py), |acc, a| {
                for key in a.cast_as::<PyDict>().unwrap().keys() {
                    match acc.get_item(key.to_string()) {
                        Some(value_) => {
                            acc.set_item(key, value_ + a.get_item(key));
                        }
                        None => {
                            acc.set_item(key, a.get_item(key));
                        }
                    }
                }
                acc
            }),
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn internal() {
//         assert!(true);
//     }
// }
