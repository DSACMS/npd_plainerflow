from dynaconf import Dynaconf
from dotenv import dotenv_values
import os

config_files = ["./test_a.env", "./test_b.env"]

# sanity check
for f in config_files:
    if not os.path.exists(f):
        raise SystemExit(f"Missing: {f}")

# Manually parse & merge .env files, then push into Dynaconf
settings = Dynaconf(envvar_prefix=False)  # no OS env overrides
for f in config_files:
    values = dotenv_values(f)  # OrderedDict
    settings.update(values, merge=True)

print(f"Dynaconf DB_HOST: {settings.get('DB_HOST')}")
print(f"Dynaconf DB_PORT: {settings.get('DB_PORT')}")
print(f"Dynaconf DB_USER: {settings.get('DB_USER')}")
print(f"Dynaconf COMMON_VAR: {settings.get('COMMON_VAR')}")

# optional assertions
assert settings.get('DB_HOST') == "remotehost"
assert settings.get('DB_PORT') == "5432"
assert settings.get('DB_USER') == "admin"
assert settings.get('COMMON_VAR') == "fileB"
print("Dynaconf loading successful!")