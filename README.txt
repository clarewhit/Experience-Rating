# Prerequisites:
	Python 3
	VS Code
	VS Code Python extension
1. Get Source Code from provided repos link
2. Open VS Code and add the source code to the workspace
3. Create a virtual environment and select interpreter (python 3.9) (https://code.visualstudio.com/docs/python/python-tutorial#_create-a-virtual-environment)
	CTRL-Shift-P
	Python: Create environment (start typing to see it)
	Venv - create virtual environment
	Select C:\Actuarial Tools\Python as interpreter
	Choose to use requirements.txt if given option
4. Open the terminal (Terminal, New Terminal) and activate the environment by using the script ".\.venv\Scripts\activate"
5. Install requirement library by using the script "pip install -r requirements.txt" (if not done in step 3)
6. To enable running from jupyter notebook: pip install -U ipykernel