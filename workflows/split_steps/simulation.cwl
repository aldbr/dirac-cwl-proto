class: Workflow
steps:
  resolve_input_data_query:
    in:
      input_data_query_parameters: simulation_step/input_data_query_parameters
    out:
    - input_data_query
    run: resolve_input_data_query.py
  simulation_step:
    in:
      parameters: parameters
    out:
    - result_sim
    - input_data_query_parameters
    run: sim.py
