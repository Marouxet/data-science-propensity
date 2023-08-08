import functions_framework
from utils import *

@functions_framework.http
def main(request):
    # user_id needed in request
    if "user_id" not in request.args:
        return "Please insert a user_id", 400
    user_id = request.args["user_id"]
    proba = get_proba(user_id)
    return proba