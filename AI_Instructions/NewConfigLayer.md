New Config Layer
==================

Recently, we added functionality to npd_plainerflow/credential_finder.py with the load_config_from_env function.

That allows the system to find an .env file and then convert that to a record in dynaconf.

This really highlights the problem with credential finder, however.

Now it is doing two things, in a very disorganized way:

* It is an sqlalchemy engine getter, using the poorly named detect_config which conflates the logic of finding various database configurations, and the process of making the engine with those credentials
* The new load_config_from_env which is capable of more generally loading configurations.

This needs to be refactored to better reflect the reality of the two phases of the project.

I would like this refactored into a new class, so that I can keep CredentialFinder around for backward compatibility. There is no need to modify, or change the npd_plainerflow/credential_finder.py file for any reason or at any stage. Do not touch it at all.

And I would like the code to live in: npd_plainerflow/confignoir.py and I want the new class name to be ConfigNoir.

To define what calling functions should experience, it should be something like this:

```psuedocode

# Handles the guessing of what credentials there are in the same way they are currently guessed..
# But returns a configuration object which will be useful for other things instead of the engine directly. 

my_dyna_config_instance = ConfigNoir.detect_and_load_config( ????? ) 

if(my_dyna_config_instance._sql_alchemy_engine):
    engine = my_dyna_config_instance._sql_alchemy_engine

    # I am off to the races here, I can use the sqlalchemy engine to connect to the database.

else:
    print("Failed to get DB engine using credentials)
    print(my_dyna_config_instance.database_connection_error_message)
    pprint(my_dyna_config_instance)
```

Then engine should be created eagerly, so that an error can be generated if needed and immediately stop script progression.

The object that should be returned should be a normal dynaconf object. This class should only have static methods in it and should build no objects. Much the same as
the current CredentialFinder class.

If no .env file is passed into the argument, then the code should follow the same path that the current CredentialFinder does, which will generate a sqlite database or do 
something else as a appropriate.

IF an array of .env files is passed in, however, then ConfigNoir should fail if those credentials cannot be used to connect to the database.
It should NOT continue to try and detect other options for credentials.

Note that when the credentials are gotten from Google Sheets.. then all of the data from the sheet should be included in the resulting config file.
Lets set a limit of 10000 rows of data, a key = value pattern, just to avoid catastrophy. If more than 10000 lines of data are there, it should result in an error being throw.

