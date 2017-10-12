



class SnakemakeRuleBuilder:

    def __init__(self, ruleName):
        self.name = ruleName
        self.inputs = {}
        self.output = ''
        self.run = ''
        self.params = {}

    def addInput(self, key, val):
        self.inputs[key] = val

    def addParam(self, key, val):
        self.params[key] = val

    def setOutput(self, val):
        self.output = val

    def setRun(self, val):
        self.run = val
        

    def __str__(self):
        ruleStr = '\nrule {}:\n'.format(self.name)
        
        inputStr = '\tinput:\n'
        for k, v in self.inputs.items():
            inputStr += '\t\t{} = {},\n'.format(k,v)
        inputStr = inputStr[:-2]+'\n' # trim last comma
        ruleStr += inputStr

        outputStr = '\toutput: {}\n'.format(self.output)
        ruleStr += outputStr

        paramStr = '\tparams:\n'
        for k, v in self.params.items():
            paramStr += '\t\t{} = {},\n'.format(k,v)
        paramStr = paramStr[:-2]+'\n' # trim last comma
        ruleStr += paramStr

        runStr = '\trun:\n{}\n'.format(self.run)
        ruleStr += runStr

        return ruleStr
