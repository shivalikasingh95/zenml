# Runtime Configuration

A ZenML pipeline clearly separated business logic from parameter configuration. Business logic is what defines 
a step and the pipeline. Step and pipeline configurations are used to dynamically set parameters at runtime. 

## Step configuration

You can easily add a configuration to a step by creating your configuration as a subclass to the BaseStepConfig. 
When such a config object is passed to a step, it is not treated like other artifacts. Instead, it gets passed
into the step when the pipeline is instantiated.

```python
from zenml.steps import step, Output, BaseStepConfig

class SecondStepConfig(BaseStepConfig):
    """Trainer params"""
    multiplier: int = 4


@step
def my_second_step(config: SecondStepConfig, input_int: int, input_float: float
                   ) -> Output(output_int=int, output_float=float):
    """Step that multiplie the inputs"""
    return config.multiplier * input_int, config.multiplier * input_float
```

The default value for the multiplier is set to 4. However, when the pipeline is instatiated you can
override the default like this:

```python
first_pipeline(step_1=my_first_step(),
               step_2=my_second_step(SecondStepConfig(multiplier=3))
               ).run()
```

This functionality is based on [Step Fixtures](#step-fixtures) which you will learn more about below.

### Setting step parameters using a config file

In addition to setting parameters for your pipeline steps in code as seen above, ZenML also allows you to use a 
configuration [yaml](https://yaml.org) file. This configuration file must follow the following structure:

```yaml
steps:
  step_name:
    parameters:
      parameter_name: parameter_value
      some_other_parameter_name: 2
  some_other_step_name:
    ...
```

For our example from above this results in the following configuration yaml.&#x20;

```yaml
steps:
  step_2:
    parameters:
      multiplier: 3
```

Use the configuration file by calling the pipeline method `with_config(...)`:

```python
first_pipeline(step_1=my_first_step(),
               step_2=my_second_step()
               ).with_config("path_to_config.yaml").run()
```

## Pipeline Run Name

When running a pipeline by calling `my_pipeline.run()`, ZenML uses the current date and time as the name for the 
pipeline run. In order to change the name for a run, simply pass it as a parameter to the `run()` function:

```python
first_pipeline_instance.run(run_name="custom_pipeline_run_name")
```

{% hint style="warning" %}
Pipeline run names must be unique, so make sure to compute it dynamically if you plan to run your pipeline multiple 
times.
{% endhint %}

Once the pipeline run is finished we can easily access this specific run during our post-execution workflow:

```python
from zenml.repository import Repository

repo = Repository()
pipeline = repo.get_pipeline(pipeline_name="first_pipeline")
run = pipeline.get_run("custom_pipeline_run_name")
```

<details>
    <summary>Minimal Code Example</summary>

```python
from zenml.steps import step, Output, BaseStepConfig
from zenml.pipelines import pipeline

@step
def my_first_step() -> Output(output_int=int, output_float=float):
    """Step that returns a pre-defined integer and float"""
    return 7, 0.1

class SecondStepConfig(BaseStepConfig):
    """Trainer params"""
    multiplier: int = 4

@step
def my_second_step(config: SecondStepConfig, input_int: int,
                   input_float: float
                   ) -> Output(output_int=int, output_float=float):
    """Step that multiply the inputs"""
    return config.multiplier * input_int, config.multiplier * input_float

@pipeline
def first_pipeline(
        step_1,
        step_2
):
    output_1, output_2 = step_1()
    step_2(output_1, output_2)

# Set configuration when executing
first_pipeline(step_1=my_first_step(),
               step_2=my_second_step(SecondStepConfig(multiplier=3))
               ).run(run_name="custom_pipeline_run_name")

# Set configuration  based on yml
first_pipeline(step_1=my_first_step(),
               step_2=my_second_step()
               ).with_config("config.yml").run()
```

With config.yml looking like this
```yaml
steps:
  step_2:
    parameters:
      multiplier: 3
```
</details>