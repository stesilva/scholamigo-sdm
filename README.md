# Neo4j User Graph Loader & Embedding Generator

This project loads structured user data into a Neo4j graph database and generates similarity embeddings between users based on their attributes and relationships. It's designed to support analysis and recommendation systems (e.g., scholarship matching) by leveraging graph-based relationships.

## ✅ Step-by-Step: How to Run the Project

### 1. Clone the Repository

git clone https://github.com/stesilva/scholamigo-sdm.git
cd scholamigo-sdm

### 2. Set Up Your Python Environment

python -m venv venv
source venv/bin/activate # On Windows: venv\Scripts\activate

### 3. Install Dependencies

pip install --upgrade pip
pip install -r requirements.txt

## 4. Environment Variables
Add a `.env` file in the project root with the following content:

NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DB=neo4j
IMPORT_PATH=/var/lib/neo4j/import
PINECONE_API_KEY = 
PINECONE_HOST=

> **Note:**  
> Ensure Neo4j has file import permissions configured in `neo4j.conf`.

## 5. Expected CSV Files

Make sure the following CSV files are inside `outputs/neo4j/`:

- `user_basic_information.csv`
- `alumni_basic_information.csv`
- `languages.csv`
- `skills.csv`
- `education.csv`
- `certificates.csv`
- `honors.csv`
- `experiences.csv`

### 6. Start Neo4j

Start Neo4j locally or via Docker:

docker run
--name neo4j
-p7474:7474 -p7687:7687
-v $HOME/neo4j/data:/data
-v $HOME/neo4j/import:/var/lib/neo4j/import
-e NEO4J_AUTH=neo4j/your_password
-d neo4j:latest

### 7. Run the Script

cd scripts

python explotation_users_data_load_neo4j.py

### 8. Done!

Successful output should look like:

 CSV files copied to Neo4j import directory.
 Connecting to Neo4j and loading data...
 Creation and loading completed with successes.
 Generating embeddings for users...
 Embeddings generated successfully.


### 9. Exploitation

To use the graph you can choose one of the 3 scripts that demonstrates possible aaplications:

- python similar_users_recommendation.py (Retrives similar users based on embeddings - Recommendation System)
- python user_alumni_recommendation.py (Retrives similar alumni based on a given user and scholarship - Recommendation System)
- python users_analytics.py (Basic queries on Cypher to retrieve data for analysis)

---

## 📌 Notes

- The graph structure follows the design in `assets/USERS_GRAPH_DESIGN.png`.
