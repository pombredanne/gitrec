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

import os
import sys
from uuid import uuid4
from java.io import IOException
from org.apache.pig.scripting import Pig

class PigWorkflowStep:
    def __init__(self, script_name, description, script_path, script_params, checkpoint_path):
        self.script_name    = script_name
        self.description    = description
        self.bound_script   = Pig.compileFromFile(script_path).bind(script_params)
        self.flag_file_path = "%s/%s.success" % (checkpoint_path,
                                                 os.path.splitext(script_name)[0])

    def validate(self):
        self.bound_script.validate()

    def run(self):
        print "%s: %s" % (self.script_name, self.description)
        stats = self.bound_script.runSingle()

        if stats.isSuccessful():
            Pig.fs("touchz %s" % self.flag_file_path)
        else:
            raise Exception("\nScript %s failed! Error should be logged above.\n" % self.script_name +
                            "Once you have fixed the problem, you can restart the workflow at this step " +
                            "using the argument \"-p CHECKPOINT=%s\"" % self.script_name)

class PigWorkflow:
    def __init__(self):
        self.steps = []

    def add_step(self, script_name, description, script_params, checkpoint_path_param="OUTPUT_PATH"):
        script_path   = "../pigscripts/%s" % script_name
        
        if checkpoint_path_param not in script_params:
            raise Exception("\nAll workflow steps must specify a path where checkpointing data will be stored.\n" +
                            "This defaults to the value of the parameter OUTPUT_PATH, or alternatively\n" + 
                            "you can specify the keyword argument checkpoint_path_param " +
                            "when calling addStep() to use a different parameter.\n")
        else:
            checkpoint_path = script_params[checkpoint_path_param]

        self.steps.append(PigWorkflowStep(script_name, description, script_path, script_params, checkpoint_path))

        return self

    def run(self, checkpoint=None):
        print
        print "Validating pigscripts"
        all_valid = True
        for step in self.steps:
            try:
                step.validate()
            except IOException, ioe:
                print "Script %s failed to validate" % (step.script_name)
                print ioe.getCause()
                raise Exception("Validation failed. Error should be logged above.")

        params = Pig.getParameters()
        checkpoint = None
        if params["CHECKPOINT"]:
            checkpoint = params.get("CHECKPOINT")

        print
        if checkpoint:
            print "Restarting workflow from failed run. Checkpoint = %s" % checkpoint
            
            found_checkpoint = False
            for step in self.steps:
                if found_checkpoint:
                    step.run()
                elif step.script_name == checkpoint and Pig.fs("test -e %s" % step.flag_file_path):
                    found_checkpoint = True
                    step.run()

            if not found_checkpoint:
                raise Exception("\nCheckpoint %s not found! " % (checkpoint) + 
                                "Please consult the logs of your failed run " +
                                "and use the checkpoint name listed there.")
        else:
            for step in self.steps:
                step.run()
