from prefect_github import GitHubRepository

repo_block = GitHubRepository(
    name="tvc2",  
    repository_url="https://github.com/pdetoni/tvc2_modelagem_de_negocio.git",  
    branch="master"  
)

repo_block.save("tvc2")
print("Bloco GitHub criado com sucesso!")