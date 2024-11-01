class Logger:
    def __init__(self, args):
        if args.verbosity == True:
            self.debug = True
        else: self.debug = False

    def log(self, *message):
        self.message = ""
        for m in message:
            self.message += str(m) + " "
        if self.debug == True:
            print(self.message)

