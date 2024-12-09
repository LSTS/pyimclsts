## PyIMCLSTS

This tool reads the IMC schema from a XML file, locally creates files containing the messages and connects (imports) the main global machinery.

See `/example` to check an example implementation of the Follow Reference maneuver.

Check the documentation page [here](https://choiwd.github.io/pyimclsts/).

### Quick start:
- Fancying a virtual env? (Not needed. Just in case you want to isolate it from your python setup)
```shell
$ sudo apt install python3.8-venv
$ python3 -m venv tutorial_env
$ source tutorial_env/bin/activate
```
- To use:
```shell
$ pip3 install pyimclsts
$ # or, if you are cloning the repo, from the folder where pyproject.toml is located:
$ pip3 install .
```
- Choose a folder and have a version of the IMC schema. Otherwise, it will fetch the latest IMC version from the LSTS git repository. Extract messages locally, with:
```shell
$ python3 -m pyimclsts.extract
```
This will locally extract the IMC.xml as python classes. You will see a folder called `pyimc_generated` which contains base messages, bitfields and enumerations from the IMC.xml file. They can be locally loaded using, for example:
```python
import pyimc_generated as pg
```
In the installed module, you will find some functions to allow you to connect to a vehicle and subscribe to messages, namely, a subscriber class.
```python
import pyimc_generated as pg
```

In the /example folder you can find scripts that use this library for various porpuses, such as reading and concatenating logs for the creation of the NetCDF files.  

## Extract Actuators 

The ``extractAct.py`` goes through a log and extrats the inputs given to the LAUV during said log. By its inputs we mean the position of the fin servos (U_SERVO, D_SERVO, L_SERVO, R_SERVO) and the proportion of the max velocity of the thruster (VEL). 
It creates a csv with the columns specified between the parenthesis which is then meant to be used to replay the log in the Gazebo simulation environment (include here link to docker file in the future). 
Before using it please input a path to the log under mission_path:

``` python 
mission_path  = "/mnt/sdb1/Missions/FRESNEL/lauv-xplore-3/20241031/073724_soi_plan"
```
And also a path to a folder you want to save the csv in the LogDataGatherer object.

```
logData = logDataGatherer(path + '/mra/ctd')
```
You can now run the script like so:

```
python3 -m example.extractAct
```













