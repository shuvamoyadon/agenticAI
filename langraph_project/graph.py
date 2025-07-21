import os
from dotenv import load_dotenv
from langgraph.graph import StateGraph,  END
from huggingface_hub import InferenceClient
from pydantic import BaseModel

# Load environment variables from .env  file
load_dotenv()

# Define your state
class MyState(BaseModel):
    user_input: str
    llm_output: str = ""

# Define a node that calls the Hugging Face LLM
def call_llm(state: MyState) -> MyState:
    # Get the token from environment variables
    hf_token = os.getenv("HUGGINGFACEHUB_API_TOKEN")
    if not hf_token:
        raise ValueError("Hugging Face API token not found in environment variables")
        
    client = InferenceClient(token=hf_token)
    model_id = "mistralai/Mistral-7B-Instruct-v0.2"
    messages = [{"role": "user", "content": state.user_input}]
    response = client.chat_completion(messages=messages, model=model_id, max_tokens=128)
    state.llm_output = response.choices[0].message.content
    return state

# Build the graph
def build_graph():
    graph = StateGraph(MyState)
    graph.add_node("llm", call_llm)
    graph.add_edge("llm", END)
    graph.set_entry_point("llm")
    return graph