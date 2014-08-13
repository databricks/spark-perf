"""Types used in configuration module for Spark's performance tests."""

class OptionSet(object):
    """Represents an option and a set of values for it to sweep over."""
    def __init__(self, name, vals, can_scale=False):
        self.name = name
        self.vals = vals
        self.can_scale = can_scale

    def scaled_vals(self, scale_factor):
        """If this class can_scale, then return scaled vals. Else return values as-is."""
        if self.can_scale:
            return [max(1, int(val * scale_factor)) for val in self.vals]
        else:
            return [val for val in self.vals]

    def to_array(self, scale_factor = 1.0):
        """
        Return array of strings each representing a command line option name/value pair.
        If can_scale is True, all values will be multipled by scale_factor before being returned.
        """
        return ["--%s=%s" % (self.name, val) for val in self.scaled_vals(scale_factor)]


class JavaOptionSet(OptionSet):
    def __init__(self, name, vals, can_scale=False):
        OptionSet.__init__(self, name, vals, can_scale)

    def to_array(self, scale_factor = 1.0):
        """Return array of strings each representing a Java option name/value pair."""
        return ["-D%s=%s" % (self.name, val) for val in self.scaled_vals(scale_factor)]

# Represents a flag-style option and a set of values that
# we will sweep over in a test run. Values can be True or False.
class FlagSet:
    def __init__(self, name, vals):
        self.name = name
        self.vals = vals
    def to_array(self, scale_factor = 1.0):
        for val in self.vals:
            assert val == True or val == False, ("FlagSet value for %s is not True or False" % 
                self.name)
        return ["--%s" % self.name if val is True else "" for val in self.vals]

# Represents an option with a constant value
class ConstantOption:
    def __init__(self, name):
        self.name = name
    def to_array(self, scale_factor = 1.0):
        return [self.name]
