#!/usr/bin/env python3
from aws_cdk import core

from sudocoins_stack.sudocoins_stack import SudocoinsStack

app = core.App()
SudocoinsStack(app, "SudocoinsStack")

app.synth()
