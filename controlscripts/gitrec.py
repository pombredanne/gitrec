"""
Copyright 2013 Mortar Data Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from org.apache.pig.scripting import Pig
from pig_checkpoint_util      import PigWorkflow

if __name__ == "__main__":
    workflow = PigWorkflow()
    params = Pig.getParameters()
    if "CHECKPOINT_PATH" not in params:
        params.put("CHECKPOINT_PATH", "../../../data/github/checkpoints")

    workflow.add_step(
        "parse_events.pig",
        "Parsing Github event logs",
        params, checkpoint_path_param="CHECKPOINT_PATH"
    ).add_step(
        "gen_item_recs.pig",
        "Generating recommendations for each item",
        params, checkpoint_path_param="CHECKPOINT_PATH"
    ).add_step(
        "gen_user_recs.pig",
        "Generating recommendations for each user",
        params, checkpoint_path_param="CHECKPOINT_PATH"
    )

    workflow.run()
