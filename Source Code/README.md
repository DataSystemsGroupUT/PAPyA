# Further Work

> This section is intended for people who want to develop or further improve PAPyA library. 

<!-- ### File Exploration</b> <br> -->
[anchor](#heading-to-achor)

- File [Rank.py](Rank.py) are used to calculate ranks for the configurations to find the best performing ones with a prescriptive analysis approach along with the validations inside the [Ranker.py](Ranker.py) file.<br>

- All the other files are methods and functions that are used by [Rank.py](Rank.py) and [Ranker.py](Ranker.py). <br>

- You can update this library by creating a new function class on these two files or create a new file if needed. <br>

### Updating Package Manager<br>

- PAPyA used python's package manager (pip) to store the library to help user find and install PAPyA easily in their environment.<br>

- To upload your updated PAPyA functionalities to the package manager, you need to have an [__init__.py](__init__.py) file and import your files along with the classes you want updated into the package manager<br>

- Then, create a [setup.py](../PAPyA%20Lib/setup.py) file in the same folder of your source code. You only need to change the version of PAPyA to update the library.<br>

- After all is setup, open terminal then run ` python3 setup.py sdist bdist_wheel `. This will create some new folders namely _build_, _dist_, and _PAPyA.egg-info_.<br>

- We want to upload all files inside the _dist_ folder to the package manager using ` pip install twine `.<br>

- With twine installed, we run the command ` twine upload dist/* ` to upload all files inside the folder.<br>

- It will then ask for your username and password of python's package manager which you will need to create first if this is your first time uploading to a python's package manager (pip).