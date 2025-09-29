# buzzline-05-kwameape

Nearly every streaming analytics system stores processed data somewhere for further analysis, historical reference, or integration with BI tools.

In this project, we incorporate relational data stores. 
We stream data into SQLite and PostgreSQL.
We use one producer that can write up to two different sinks:

- to a file
- to a Kafka topic (set in .env)

In data pipelines:

- A **source** generates records (our message generator). 
- A **sink** stores or forwards them (file, Kafka, SQLite, DuckDB). 
- An **emitter** is a small function that takes data from a source and writes it into a sink. 
- Each emitter has one job (`emit_message` to the specified sink). 

--- 

## First, Verify tool installation and Setup

1. VS Code.
2. Python. **Python 3.11 is required.**
3. Wsl and Kafka.
4. PostgreSQL.

## Second, Copy This Example Project & Rename

1. Once the tools are installed, project is copied/forked into my GitHub account and my version of this project is cloned to my local machine to run and experiment with.
2. It is named `buzzline-05-kwameape`

Additional information about our standard professional Python project workflow is available at
<https://github.com/denisecase/pro-analytics-01>. 
    
---

## Task 0. If Windows, Start WSL

Launch WSL. Open a PowerShell terminal in VS Code. Run the following command:

```powershell
wsl
```

You should now be in a Linux shell (prompt shows something like `username@DESKTOP:.../repo-name$`).

Do **all** steps related to starting Kafka in this WSL window.

---

## Task 1. Start Kafka (using WSL if Windows)

In P2, you downloaded, installed, configured a local Kafka service.
Before starting, run a short prep script to ensure Kafka has a persistent data directory and meta.properties set up. This step works on WSL, macOS, and Linux - be sure you have the $ prompt and you are in the root project folder.

1. Make sure the script is executable.
2. Run the shell script to set up Kafka.
3. Cd (change directory) to the kafka directory.
4. Start the Kafka server in the foreground. Keep this terminal open - Kafka will run here.

```bash
chmod +x scripts/prepare_kafka.sh
scripts/prepare_kafka.sh
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties
```

**Keep this terminal open!** Kafka is running and needs to stay active.

For detailed instructions, see [SETUP_KAFKA](https://github.com/denisecase/buzzline-02-case/blob/main/SETUP_KAFKA.md) from Project 2. 

---

## Task 2. Manage Local Project Virtual Environment

Open your project in VS Code and use the commands for your operating system to:

1. Create a Python virtual environment.
2. Activate the virtual environment.
3. Upgrade pip and key tools. 
4. Install from requirements.txt.

### Windows

Open a new PowerShell terminal in VS Code (Terminal / New Terminal / PowerShell).
**Python 3.11** is required for Apache Kafka. 

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip wheel setuptools
py -m pip install --upgrade -r requirements.txt
```

If you get execution policy error, run this first:
`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

### Mac / Linux

Open a new terminal in VS Code (Terminal / New Terminal)

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r requirements.txt
```

---

## Task 3. Write New Emitter Related Functions.
Write functions that will be used in producer module to send data to file and kafka topic. These
functions located in  **kafka_emitter.py** and **file_emitter.py**

---

## Task 4. Start a New Streaming Application

This will take two terminals:

1. One to run the producer which writes messages to a file and Kafka topic. 
2. Another to run each consumer. One consumer to consumer from file and another
to consume from kafka topic.

### Producer Terminal (Outputs to Various Sinks)

Start the producer to generate the messages. 

The producer writes messages to a live data file in the data folder.
If the Kafka service is running, it will try to write the messages to a Kafka topic as well.
For configuration details, see the .env file. 

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer. 

Windows:

```shell
.\.venv\Scripts\Activate.ps1
py -m producers.producer_kwameape
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m producers.producer_kwameape
```

NOTE: The producer will still work if the Kafka service is not available.

### Consumer Terminal

Start an associated consumer. 
You have options. 

1. Start the consumer that reads from the live data file.
2. Start the consumer that reads from the Kafka topic.

NOTE: Each consumer modify or transforms the message read from the
life or kafka topic and then sends the transformed data to a SQLite database
as well as a PostgreSQL database. The transform employed was to get of the timestamp
feature and remove whitespaces in messages.

In VS Code, open a NEW terminal in your root project folder. 
Use the commands below to activate .venv, and start the consumer. 

Windows:
```shell
.\.venv\Scripts\Activate.ps1
py -m consumers.kafka_consumer_kwameape
OR
py -m consumers.file_consumer_kwameape

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m consumers.kafka_consumer_arnold
OR
python3 -m consumers.file_consumer_arnold
```
---

## Data Summarize
In the **data_summaries** folder, we have **postgresql_select.py** and **sqlite_select.py**, python modules that can used to create summary tables in PostgreSQL AND SQLite respectively. The module makes use of SQL
select statements.

## How To Stop a Continuous Process

To kill the terminal, hit CTRL c (hold both CTRL key and c key down at the same time).

## Later Work Sessions

When resuming work on this project:

1. Open the project repository folder in VS Code. 
2. Start the Kafka service (use WSL if Windows) and keep the terminal running. 
3. Activate your local project virtual environment (.venv) in your OS-specific terminal.
4. Run `git pull` to get any changes made from the remote repo (on GitHub).

## After Making Useful Changes

1. Git add everything to source control (`git add .`)
2. Git commit with a -m message.
3. Git push to origin main.

```shell
git add .
git commit -m "your message in quotes"
git push -u origin main
```

## Save Space

To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later.
Managing Python virtual environments is a valuable skill.

## License

This project is licensed under the MIT License as an example project.
You are encouraged to fork, copy, explore, and modify the code as you like.
See the [LICENSE](LICENSE.txt) file for more.

## Recommended VS Code Extensions

- Black Formatter by Microsoft
- Markdown All in One by Yu Zhang
- PowerShell by Microsoft (on Windows Machines)
- Python by Microsoft
- Python Debugger by Microsoft
- Ruff by Astral Software (Linter + Formatter)
- **SQLite Viewer by Florian Klampfer**
- WSL by Microsoft (on Windows Machines)
