# LangGraph Agent AI Solution

This project implements a simple yet powerful AI agent workflow using LangGraph and integrates with Hugging Face's inference API for language model capabilities.

## Project Structure

- `graph.py`: Core implementation of the LangGraph state machine
- `main.py`: Entry point for running the LangGraph workflow
- `requirements.txt`: Project dependencies

## Components

### 1. LangGraph State Machine (`graph.py`)

This file defines the core state machine for the AI agent:

- **MyState**: A Pydantic model that defines the state structure
  - `user_input`: Stores the user's input prompt
  - `llm_output`: Stores the generated response from the LLM

- **call_llm()**: A node function that:
  - Takes the current state as input
  - Uses Hugging Face's InferenceClient to call the Mistral-7B-Instruct model
  - Updates the state with the model's response
  - Returns the updated state

- **build_graph()**: Configures the state machine:
  - Creates a new StateGraph instance
  - Adds the LLM node
  - Sets up the execution flow (currently a simple linear flow)

### 2. Main Application (`main.py`)

The entry point of the application that:
1. Takes user input from the command line
2. Initializes the LangGraph state machine
3. Executes the graph with the provided input
4. Prints the LLM's response

## Workflow

1. The user provides input through the command line
2. The main script initializes the LangGraph state machine
3. The state machine processes the input through the LLM node
4. The response is returned to the user

## Setup and Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up your Hugging Face API token in the environment or directly in the code

## Usage

### Running the LangGraph Agent
```bash
python main.py
```

## Dependencies

- langgraph
- huggingface-hub
- pydantic

## Security Note

⚠️ **Important**: The current implementation includes a hardcoded API key in `graph.py`. In a production environment, always use environment variables or a secure secrets management system to store sensitive credentials.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
