from pinecone import Pinecone, ServerlessSpec
from flask import Flask, request, jsonify
import numpy as np
from groq import Groq
from config import get_config

config = get_config()

app = Flask(__name__)

pc = Pinecone(api_key=config['pinecone_api'])

# Create retriver index
index_name = "user-vectors"
retriver = pc.Index(index_name)

def convert_catergorical(encoder):
    result = {}
    occupations = [ "administrator", "artist", "doctor", "educator", "engineer", "entertainment", "executive", "healthcare", "homemaker", "lawyer", "librarian", "marketing", "none", "other", "programmer", "retired", "salesman", "scientist", "student", "technician", "writer" ]
    if encoder[6] == 1:
        result['gender'] = 'Female'
    elif encoder[7] == 1:
        result['gender'] = 'Male'

    id_occupation = np.where(np.array(encoder[8:29]) > 0)[0][0]
    occupation = occupations[id_occupation]
    result['job'] = occupation

    return result

def generate_explaination(user1, user2, client):
    user1 = convert_catergorical(user1)
    user2 = convert_catergorical(user2)
    # Generate the explanation using the LangChain

    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": f"""
        I have two users with the following details:
        User 1: 
        - Gender: {user1['gender']}
        - Job: {user1['job']}
        
        User 2: 
        - Gender: {user2['gender']}
        - Job: {user2['job']}
        
        Please explain why this recommendation why 2 users should me made , don't show they are user1 or user2, just make it easy, make a small context, just 1 line.
        """,
            }
        ],
        model="llama3-8b-8192",
    )
    return chat_completion.choices[0].message.content

@app.route('/matching/<userid>', methods=['GET'])
def get_similarity_users(userid):
    """
    API endpoint to retrieve similar users for a given user ID.

    Args:
        userid (str): The ID of the user for whom to find similar users.

    Returns:
        JSON: A list of dictionaries containing information about similar users,
            including their ID, score, and an explanation for the recommendation.
    """
    results = []
    list_similarity = retriver.query(top_k=11, id=str(userid),include_metadata=True, include_values=True)['matches']
    core_vector = list_similarity[0]

    client = Groq(
        api_key=config['groq_api']
    )

    # Generate explainable recommendation sytem
    for user_vector in list_similarity[2:]:
        result = {}
        expalinable_recommend = generate_explaination(core_vector.values, user_vector.values, client)
        
        result['id'] = user_vector.id
        result['score'] = user_vector.score
        result['explaination'] = expalinable_recommend

        results.append(result)
    
    return jsonify(results)

if __name__ == "__main__":
    app.run(debug=True)








