from graph import build_graph, MyState

##  main code
if __name__ == "__main__":
    user_input = input("Enter your prompt:")
    graph = build_graph()
    state = MyState(user_input=user_input)
    compiled_graph = graph.compile()
    result = compiled_graph.invoke(state)
    print("LLM Output:", result["llm_output"])
