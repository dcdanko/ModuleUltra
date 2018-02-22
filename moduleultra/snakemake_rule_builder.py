
class SnakemakeRuleBuilder:
    '''Simplistic class to build a snakemake rule string.'''

    def __init__(self, ruleName):
        self.name = ruleName
        self.inputs = None
        self.output = ''
        self.run = ''
        self.params = {}
        self.local = True

    def addInput(self, *args):
        '''Add an input to the rule.

        Accepts one `arg` to create a list of inputs.
        Two `args` to create key value pairs of inputs.
        After being given one or two `args` will not accept the other value.
        '''

        if len(args) == 2:
            if self.inputs is None:
                self.inputs = {}
            self.inputs[args[0]] = args[1]
        elif len(args) == 1:
            if self.inputs is None:
                self.inputs = []
            self.inputs.append(args[0])

    def addParam(self, key, val):
        '''Add a named parameter to the rule.'''
        self.params[key] = val

    def setOutput(self, val):
        '''Add an unamed output to the rule.'''
        self.output = val

    def setRun(self, val):
        '''Add a script to run to the rule.'''
        self.run = val

    def __str__(self):
        ruleStr = ''
        if self.local:
            ruleStr += '\nlocalrules: {}\n'.format(self.name)
        ruleStr += '\nrule {}:\n'.format(self.name)

        inputStr = '\tinput:\n'
        if type(self.inputs) == dict:
            for k, v in self.inputs.items():
                inputStr += '\t\t{} = "{}",\n'.format(k, v)
        elif type(self.inputs) == list:
            for el in self.inputs:
                inputStr += '\t\t"{}",\n'.format(el)

        inputStr = inputStr[:-2] + '\n'  # trim last comma
        ruleStr += inputStr

        if len(self.output) > 0:
            outputStr = '\toutput: "{}"\n'.format(self.output)
            ruleStr += outputStr

        if len(self.params) > 0:
            paramStr = '\tparams:\n'
            for k, v in self.params.items():
                paramStr += '\t\t{} = "{}",\n'.format(k, v)
            paramStr = paramStr[:-2] + '\n'  # trim last comma
            ruleStr += paramStr

        if len(self.run) > 0:
            runStr = '\trun:\n{}\n'.format(self.run)
            ruleStr += runStr

        return ruleStr
