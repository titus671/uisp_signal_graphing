class Logger:
    def __init__(self):
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("-v", "--verbosity", default=0, action="store_true", help="increase output verbosity")
        args = parser.parse_args() 
        if args.verbosity == True:
            self.debug = True
        else: self.debug = False

    def log(self, *message):
        self.message = ""
        for m in message:
            self.message += str(m) + " "
        if self.debug == True:
            print(self.message)

