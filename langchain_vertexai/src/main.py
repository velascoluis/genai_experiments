import os
os.environ["GOOGLE_API_KEY"] = "TO_DO_DEVELOPER"  
os.environ["GOOGLE_API_KEY"] = "TO_DO_DEVELOPER"
os.environ["GOOGLE_CSE_ID"] = "TO_DO_DEVELOPER"

import langchain
llm = langchain.llms.vertexai.VertexAI(temperature=0)
llm_math_chain = langchain.LLMMathChain(llm=llm, verbose=True)
search = langchain.utilities.GoogleSearchAPIWrapper()
python_repl = langchain.utilities.PythonREPL()

tools = [
    langchain.agents.Tool(
        name = "Search",
        func=search.run,
        description="useful for when you need to answer questions about current events or the current state of the world\nDO NOT USE THIS FOR SIMPLE QUESTIONS!"
    ),
    langchain.agents.Tool(
        name="Calculator",
        func=llm_math_chain.run,
        description="useful for when you need to answer questions about mathematically related topics\nUSE ONLY FOR MATH RELATED QUESTIONS!"
    ),
    langchain.agents.Tool(
        name="Python REPL",
        func=python_repl.run,
        description = "A Python shell. Use this to execute python commands. "
        "Input should be a valid python command. "
        "If you want to see the result, you should print it out "
        "with `print(...)`."
    )
]

agent = langchain.agents.initialize_agent(tools, llm, agent=langchain.agents.AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=True)
agent.run("Add 10 years to the year in which SNES was released in Europe")
