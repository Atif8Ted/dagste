# dagster
## TO execute :
* Dagster Python API

```python
if __name__ == "__main__":
    execute_pipeline(hello_pipeline)   # Hello, dagster!
```
* Dagster CLI

```bash
dagster pipeline execute -f hello_dagster.py
``` 
* Dagit web UI

```bash
dagit -f hello_dagster.py
```
**And then navigate to http://localhost:3000 to start using Dagit**