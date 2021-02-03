from flask import Flask
from flask import request
from flask import render_template
import pymongo
teach = Flask(__name__, static_url_path='/static', static_folder='./static')  # __name__ 代表目前執行的模組

@teach.route("/wowpeople")
def home():
    return render_template("index3.html")

@teach.route("/joinus", methods=["GET", "POST"])
def joinus():
    addMemberSTR = '''
    
    <html>
        <head>
            <title>吾人商店</title>
            <meta charset="utf-8" />		    
		    <meta name="viewport" content="width=device-width, initial-scale=1, user-scalable=no" />
		    <link rel="stylesheet" href="./static/assets8/css/main.css" />
		    <noscript><link rel="stylesheet" href="./static/assets8/css/noscript.css" /></noscript>
		    <link rel="icon" href="{{ url_for('static', filename='img/22.jpg') }}">
        <head>
        
        <body >
             <div id="wrapper">

					<div id="main">

						<!-- Post -->
							<section class="post">
								<header class="major">
								<h1>加入會員<br /></h1>
								</header>

								<!-- Form -->
									

									<form method="post" action="#">
										<div class="row gtr-uniform">
											<div class="col-6 col-12-xsmall">
												<input type="text" name="member_name" id="member_name" value="" placeholder="Name" />
											</div>
											<div class="col-6 col-12-xsmall">
												<input type="text" name="member_age" id="member_age" value="" placeholder="Age" />
											</div>
											<div class="col-6 col-12-xsmall">
												<input type="email" name="member_email" id="member_email" value="" placeholder="Email" />
											</div>

											<div class="col-6 col-12-xsmall">
												<input type="text" name="member_income" id="member_income" value="" placeholder="Income" />
											</div>

											<!-- Break -->
											<div class="col-12">
												<select name="Kid" id="Kid">
													<option value="">- Kid -</option>
													<option value="0">沒有</option>
													<option value="1">1個</option>
													<option value="2">2個</option>
													<option value="3+">3個以上</option>
												</select>

											</div>

											<div class="col-12">
												<select name="number" >
													<option value="">- Household -</option>
													<option value="1">1名成員</option>
													<option value="2">2名成員</option>
													<option value="3">3名成員</option>
													<option value="4">4名成員</option>
													<option value="5+">5名以上成員</option>
												</select>
											</div><br>

											<!-- Break -->
											<div class="col-4 col-12-small">
												<input type="radio" id="married" name="member_maritalstatus" value="married" checked=>
												<label for="married">已婚</label>
											</div>
											<div class="col-4 col-12-small">
												<input type="radio" id="unmarried" name="member_maritalstatus" value="unmarried" >
												<label for="unmarried">未婚</label>
											</div>
											

											<!-- Break -->

											<div class="col-6 col-12-small">
												<label >可以上傳個人照片</label>
												<input type="file" name="photo">

											</div>

											<!-- Break -->

											<!-- Break -->
											<div class="col-12">
												<ul class="actions">
													<li><input type="submit" value="送出資料" class="primary" /></li>
													<li><input type="reset" value="重新填寫" /></li>
												</ul>
											</div>
										</div>
									</form>					
							</section>
					</div>
			</div>
            
            <script src="/static/assets8/js/jquery.min.js"></script>
			<script src="/static/assets8/js/jquery.scrollex.min.js"></script>
			<script src="/static/assets8/js/jquery.scrolly.min.js"></script>
			<script src="/static/assets8/js/browser.min.js"></script>
			<script src="/static/assets8/js/breakpoints.min.js"></script>
			<script src="/static/assets8/js/util.js"></script>
			<script src="/static/assets8/js/main.js"></script>
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
        kid = request.form.get("Kid")

        member = {
            "Name": member_name,
            "Age": member_age,
            "Email": member_email,
            "Marriage": member_maritalstatus,
            "Income": member_income,
            "Family": number,
            "Kid": kid
        }

        client = pymongo.MongoClient(
            "mongodb+srv://peter:0987602620@cluster0.0qqo9.mongodb.net/ceb101?retryWrites=true&w=majority")


        # myclient = pymongo.MongoClient('mongodb://172.20.10.6:27017')
        mydb = client.wow
        mycol = mydb['members']
        mycol.insert_many([member])
        return """
        <html>
            <head>	
		        <meta charset="utf-8" />
		        <meta name="viewport" content="width=device-width, initial-scale=1" />
		        <link rel="stylesheet" href="/static/assets9/css/main.css" >
		        <link rel="icon" href="{{ url_for('static', filename='img/22.jpg') }}">
	        </head>
	        <body>
	            
				    <h1  style="text-align:center;">您已成功<br>加入會員</h1>
					
					<center><input type="button" value="Go Back" style="width:200px;height:50px;font-size:20px;"  onclick="location.href='https://pyhton-teach.herokuapp.com/joinus'">	</center>								
					
                    
	        </body>
	    </html>
        """
# @teach.route("/right-sidebar")
# def home5():
#     return render_template("right-sidebar.html")
# @teach.route('/wowpeople')
# def home():
#     return teach.send_static_file('index2.html')
if __name__=="__main__":   # 如果以主程式執行
    teach.debug = True
    teach.run()  # 立刻啟動伺服器