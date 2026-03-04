import json
import os
from pathlib import Path

import dagster as dg

from dagster_essentials.defs import jobs


@dg.sensor(
    job=jobs.adhoc_request_job,
)
def sensors(context: dg.SensorEvaluationContext) -> dg.SensorResult:
    path_to_requests = Path(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "data",
            "requests",
        )
    )

    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}

    runs_for_unseen_files = []

    for filename in os.listdir(path_to_requests):
        file_path = path_to_requests.joinpath(filename)

        if file_path.suffix != ".json" or not file_path.is_file:
            # Skip anything that isn't a json file
            continue

        last_modified = os.path.getmtime(file_path)

        current_state[filename] = last_modified

        if filename in previous_state and previous_state[filename] == last_modified:
            # Skip files that have been seen previously with the same last modified timestamp
            continue

        with open(file_path, "r") as file:
            request_config = json.load(file)

            # TODO short explanation of why this key
            adhoc_request_key = f"adhoc_request_{filename}_{last_modified}"

            # TODO short explanation of what the hell this is
            adhoc_request_config = {
                "ops": {
                    "adhoc_request": {
                        "config": {
                            "filename": filename,
                            **request_config,
                        }
                    }
                }
            }

            runs_for_unseen_files.append(
                dg.RunRequest(
                    run_key=adhoc_request_key, run_config=adhoc_request_config
                )
            )

    return dg.SensorResult(
        cursor=json.dumps(current_state),
        run_requests=runs_for_unseen_files,
    )
