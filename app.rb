require 'sinatra/base'
require 'mysql2'
require 'mysql2-cs-bind'
require 'erubis'
require 'dalli'

module Ishocon1
  class AuthenticationError < StandardError; end
  class PermissionDenied < StandardError; end
end

class Ishocon1::WebApp < Sinatra::Base
  session_secret = ENV['ISHOCON1_SESSION_SECRET'] || 'showwin_happy'
  use Rack::Session::Cookie, key: 'rack.session', secret: session_secret
  set :erb, escape_html: true
  set :public_folder, File.expand_path('../public', __FILE__)
  set :protection, true

  helpers do
    def config
      @config ||= {
        db: {
          host: ENV['ISHOCON1_DB_HOST'] || 'localhost',
          port: ENV['ISHOCON1_DB_PORT'] && ENV['ISHOCON1_DB_PORT'].to_i,
          username: ENV['ISHOCON1_DB_USER'] || 'ishocon',
          password: ENV['ISHOCON1_DB_PASSWORD'] || 'ishocon',
          database: ENV['ISHOCON1_DB_NAME'] || 'ishocon1'
        }
      }
    end

    def db
      return Thread.current[:ishocon1_db] if Thread.current[:ishocon1_db]
      client = Mysql2::Client.new(
        host: config[:db][:host],
        port: config[:db][:port],
        username: config[:db][:username],
        password: config[:db][:password],
        database: config[:db][:database],
        reconnect: true
      )
      client.query_options.merge!(symbolize_keys: true)
      Thread.current[:ishocon1_db] = client
      client
    end

    def dalli
      return Thread.current[:ishocon1_mem] if Thread.current[:ishocon1_mem]
      client = Dalli::Client.new('127.0.0.1:11211')
      Thread.current[:ishocon1_mem] = Dalli::Client.new('127.0.0.1:11211')
      client
    end

    def cache_top_page_products
      return if dalli.get("top_products_page_0")
      puts "start load top_products_pages"
      200.times do | page |
        to = 10000 - (page * 50)
        from = 10000 - (50 + page * 50) + 1
        products = db.xquery("SELECT id, name, image_path, price, description FROM products WHERE id BETWEEN #{from} AND #{to} ORDER BY id DESC")
        ah_p = products.map { |p| { id: p[:id], name: p[:name], image_path: p[:image_path], price: p[:price], description:p[:description] } }
        dalli.set("top_products_page_#{page}", ah_p)
      end
    end

    def time_now_db
      Time.now - 9 * 60 * 60
    end

    def authenticate(email, password)
      user = db.xquery('SELECT * FROM users WHERE email = ?', email).first
      fail Ishocon1::AuthenticationError unless user[:password] == password
      session[:user_id] = user[:id]
    end

    def authenticated!
      fail Ishocon1::PermissionDenied unless current_user
    end

    def current_user
      db.xquery('SELECT * FROM users WHERE id = ?', session[:user_id]).first
    end

    def buy_product(product_id, user_id)
      db.xquery('INSERT INTO histories (product_id, user_id, created_at) VALUES (?, ?, ?)', \
        product_id, user_id, time_now_db)
    end

    def already_bought?(product_id)
      return false unless current_user
      h = db.xquery('SELECT * FROM histories WHERE product_id = ? AND user_id = ?', product_id, current_user[:id]).first
      !(h.nil?)
    end

    def create_comment(product_id, user_id, content)
      db.xquery('INSERT INTO comments (product_id, user_id, content, created_at) VALUES (?, ?, ?, ?)', \
        product_id, user_id, content, time_now_db)
    end
  end

  error Ishocon1::AuthenticationError do
    session[:user_id] = nil
    halt 401, erb(:login, layout: false, locals: { message: 'ログインに失敗しました' })
  end

  error Ishocon1::PermissionDenied do
    halt 403, erb(:login, layout: false, locals: { message: '先にログインをしてください' })
  end

  get '/login' do
    session.clear
    erb :login, layout: false, locals: { message: 'ECサイトで爆買いしよう！！！！' }
  end

  post '/login' do
    authenticate(params['email'], params['password'])
    redirect '/'
  end

  get '/logout' do
    session[:user_id] = nil
    session.clear
    redirect '/login'
  end

  get '/' do
    page = params[:page].to_i || 0

    products = dalli.get("top_products_page_#{page}")
    product_ids = products.map { |p| p[:id] }
    cmt_query = <<SQL
SELECT 
	p.id as p_id, 
        c.id as c_id,
        c.content as c_content,
        u.name as u_name
FROM 
	products as p
INNER JOIN 
	comments as c
ON 
	c.product_id = p.id
INNER JOIN 
	users as u
ON 
	c.user_id = u.id
WHERE 
	p.id IN (?)
ORDER BY 
	p.id ASC,
	c.id DESC
SQL

    # TODO コメント数を取りたいがためにViewで発行 N+1しててヤバイ
    # commentsにカウンターキャッシュレコードを入れるとか。
    # キャッシュするとか。配列サイズを数えるだけになったのでいらないかも？
    # cmt_count_query = 'SELECT count(*) as count FROM comments WHERE product_id = ?'
    cmts = db.xquery(cmt_query, product_ids)
    c_h = {}
    cmts.map do | c | 
      key = c[:p_id].to_s
      c_h[key] ||= []
      c_h[key] << { id: c[:c_id], content: c[:c_content], user_name: c[:u_name] }
    end

    erb :index, locals: { products: products, comments: c_h }
  end

  get '/users/:user_id' do
    products_query = <<SQL
SELECT
  p.id, p.name, p.description, p.image_path, p.price, h.created_at
FROM
  (SELECT id, product_id, created_at FROM histories WHERE user_id = ? ORDER BY id DESC ) as h
INNER JOIN
 products as p
ON h.product_id = p.id
SQL
    products = db.xquery(products_query, params[:user_id])

    total_pay = 0
    products.each do |product|
      total_pay += product[:price] || 0
    end

    user = db.xquery('SELECT * FROM users WHERE id = ?', params[:user_id]).first
    erb :mypage, locals: { products: products, user: user, total_pay: total_pay }
  end

  get '/products/:product_id' do
    product = db.xquery('SELECT * FROM products WHERE id = ?', params[:product_id]).first
    comments = db.xquery('SELECT * FROM comments WHERE product_id = ?', params[:product_id])
    erb :product, locals: { product: product, comments: comments }
  end

  post '/products/buy/:product_id' do
    authenticated!
    buy_product(params[:product_id], current_user[:id])
    redirect "/users/#{current_user[:id]}"
  end

  post '/comments/:product_id' do
    authenticated!
    create_comment(params[:product_id], current_user[:id], params[:content])
    redirect "/users/#{current_user[:id]}"
  end

  get '/initialize' do
    db.query('DELETE FROM users WHERE id > 5000')
    db.query('DELETE FROM products WHERE id > 10000')
    db.query('DELETE FROM comments WHERE id > 200000')
    db.query('DELETE FROM histories WHERE id > 500000')

    # for TopPage Product
    cache_top_page_products

    "Finish"
  end
end
