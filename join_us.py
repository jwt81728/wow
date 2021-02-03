import pymongo
from flask import Flask, request

app = Flask(__name__)

#會員註冊
@app.route("/joinus", methods=["GET", "POST"])
def joinus():

    addMemberSTR = '''
    <html>
        <head>
            <title>吾人商店</title>
            <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
        <head>
        <body>
            <h1><center>加入會員</center></h1>
            <form action="/joinus" method="POST">
                <table align="center" style="border:2px green solid;border-radius:5px;width:370px;height:50px;background-color:#eee;" >
                    <tr>
                        <td width="125" height="35">Name：</td>
                        <td width="300" ><input type="textbox" name="member_name"/></td>
                    </tr>
                    
                    <tr>
                        <td width="125" height="35">Age：</td>
                        <td width="300"><input type="textbox" name="member_age"/></td>
                    </tr>
                    <tr>
                        <td width="125" height="35">Email：</td>
                        <td width="300">
                            <input type="textbox" name="member_email">
                        </td>
                    </tr>
                    <tr>
                        <td width="125" height="35">Marital_status：</td>
                        <td width="300">
                            <input type="radio" name="member_maritalstatus" value="married" checked="true">已婚
                            <input type="radio" name="member_maritalstatus" value="unmarried">未婚
                        </td>
                    </tr>
                    <tr>
                        <td width="125" height="35">Income：</td>
                        <td width="300">
                            <input type="textbox" name="member_income">
                        </td>
                    </tr>
                    <tr>
                        <td width="125" height="35">Household：</td>
                        <td width="300">
                            <select name="number">
                                <option value="1">1
                                <option value="2">2
                                <option value="3">3
                                <option value="4">4
                                <option value="5+">5+
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <td width="125" height="35">Kid：</td>
                        <td width="300">
                            <select name="kid">
                                <option value="0">0
                                <option value="1">1
                                <option value="2">2
                                <option value="3+">3+
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <td width="125" height="35">Photo：</td>
                        <td width="300">
                            <input type="file" name="photo">
                        </td>
                    </tr>
                    <tr>
                        <td width="125" height="35" colspan="2">
                            <center>
                                <input type="submit" value="送出資料">
                                <input type="reset" value="重新填寫">
                            </center>
                        </td>
                    </tr>
			    </table>

            </form>
        </body>
    </html>
    '''
    if request.method == 'GET':
        return addMemberSTR
    elif request.method == 'POST':
        member_name = request.form.get("member_name")
        member_age = request.form.get("member_age")
        member_email = request.form.get("member_email")
        member_maritalstatus = request.form.get("member_maritalstatus")
        member_income = request.form.get("member_income")
        number = request.form.get("number")
        kid = request.form.get("kid")

        member = {
            "Name": member_name,
            "Age": member_age,
            "Email": member_email,
            "Marriage": member_maritalstatus,
            "Income": member_income,
            "Family": number,
            "Kid": kid
        }

        myclient = pymongo.MongoClient('mongodb://10.1.1.131:27017')
        mydb = myclient['NobodyShop']
        mycol = mydb['Member']
        mycol.insert_many([member])

        return """
        <html>
        <head>
            <title>會員資料</title>
        <head>
        <body>
            <center><h1>加入會員成功</h1></center>
        </body>
        """

if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=5000)


