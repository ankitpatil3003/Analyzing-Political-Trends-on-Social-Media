import requests
import nltk
CONF_THRESH = 0.9


def clean_text(text):
    """ Cleans text for hate speech api insert 
        Args :
            text (str) : Text containing comment to clean
        Returns :
            str : cleaned text
     """
    # Tokenize and remove stopwords using NLTK
    nltk.download('punkt')
    nltk.download('stopwords')
    words = nltk.word_tokenize(text)
    stop_words = set(nltk.corpus.stopwords.words('english'))
    filtered_words = [word for word in words if word.lower() not in stop_words]

    # Reconstruct cleaned text
    cleaned_text = ' '.join(filtered_words)
    return cleaned_text

def get_score(text):
    """
    Gets the score of a given text from Hate Speech API
    Args:
        text (str) : Cleaned data for hate speech api 
    """
    token = "348517cfb8e88b8e9470b89de08d599f"
    data = {
        'token': token,
        'text' : clean_text(text)
    }
    res = requests.post("https://api.moderatehatespeech.com/api/v1/moderate/", json=data).json()
    # if res.status_code != 200 :
    #     raise Exception('moderate api failed')
    if res["class"] == "flag" and float(res["confidence"]) > CONF_THRESH:
    # Do something
        return True
  
    return False