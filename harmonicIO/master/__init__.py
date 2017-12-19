import importlib

falcon_spec = importlib.util.find_spec("falcon")
if falcon_spec is None:
    raise Exception("Falcon module has not been installed.")

# general_spec = importlib.util.find_spec("general")
# if general_spec is None:
#     raise Exception("General module is missing.")
#
