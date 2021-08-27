#!/usr/bin/env python3
from aws_cdk import core

from sudocoins_stack.sudocoins_stack import SudocoinsStack

app = core.App()
SudocoinsStack(
    app,
    'SudocoinsStack',
    env=core.Environment(account='977566059069', region='us-west-2')
)

app.synth()
