require 'sinatra/base'
require 'mysql2'
require 'mysql2-cs-bind'
require 'erubis'
require 'dalli'
require 'rack-lineprof'
 
module Ishocon1
  class AuthenticationError < StandardError; end
  class PermissionDenied < StandardError; end
end

class Ishocon1::WebApp < Sinatra::Base
  #use Rack::Lineprof, profile: 'app.rb'


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
      puts "end load top_products_pages"
    end

    def cache_top_page_comments
      return if dalli.get("product_1_comments")

      puts "start load top_comments_pages"
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
ORDER BY
       p.id ASC,
       c.id DESC
SQL
      # こんなのをつくる
      # product_1_comments = [{content:xx, user_name:xx},content:}]
      cmts = db.xquery(cmt_query)
      cmts.map do | c |
        key = "product_#{c[:p_id].to_s}_comments"
        arr = dalli.get(key)
        arr ||= []
        arr << { content: c[:c_content], user_name: c[:u_name] }
        dalli.set(key, arr)
      end

      puts "end load top_comments_pages"
    end

    def cache_users
      return if dalli.get("user_1")

      puts "start load cache_users"
      db.xquery('SELECT * FROM users').each do | u |
        dalli.set("user_#{u[:id]}", u)
      end
      puts "end load cache_users"
    end

    def cache_products
      return if dalli.get("product_1")
      puts "start load cache_products"
      db.xquery('SELECT * FROM products').each do | p |
        dalli.set("product_#{p[:id]}", p)
      end
      puts "end load cache_products"
    end

    def cache_user_buy_histories
      return if dalli.get("user_1_buy_histories")

      puts "start load user_buy_histories"

      products_query = <<SQL
SELECT
  h.user_id as h_user_id,
  p.id as p_id,
  p.name as p_name,
  p.description as p_description,
  p.image_path as p_image_path,
  p.price as p_price,
  h.created_at as h_created_at
FROM
   histories as h
INNER JOIN
 products as p
ON h.product_id = p.id
ORDER BY h.user_id ASC, h.id DESC
SQL
      db.xquery(products_query).each do | p |
        key = "user_#{p[:h_user_id]}_buy_histories"
        arr = dalli.get(key)
        arr ||= []
        arr << {
          id: p[:p_id],
          name: p[:p_name],
          description: p[:p_description],
          image_path: p[:p_image_path],
          price: p[:p_price],
          created_at: p[:h_created_at]
        }
        dalli.set(key, arr)
      end

      puts "end load user_buy_histories"
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
      dalli.get("user_#{session[:user_id]}")
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
    erb :index, locals: { products: products}
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

    user = dalli.get("user_#{params[:user_id]}")
    erb :mypage, locals: { products: products, user: user, total_pay: total_pay }
  end

  get '/products/:product_id' do
    product = dalli.get("product_#{params[:product_id]}")
    erb :product, locals: { product: product }
  end

  post '/products/buy/:product_id' do
    authenticated!
    buy_product(params[:product_id], current_user[:id])
    redirect "/users/#{current_user[:id]}"
  end

  post '/comments/:product_id' do
    authenticated!
    user = current_user
    key = "product_#{params[:product_id].to_s}_comments"
    arr = dalli.get(key)
    arr ||= []
    arr.unshift ( { content: params[:content], user_name: user[:name] } )
    dalli.set(key, arr)
    redirect "/users/#{user[:id]}"
  end

  get '/initialize' do
    db.query('DELETE FROM users WHERE id > 5000')
    db.query('DELETE FROM products WHERE id > 10000')
    db.query('DELETE FROM comments WHERE id > 200000')
    db.query('DELETE FROM histories WHERE id > 500000')

    # for TopPage Product
    cache_top_page_products

    # for TopPage Comments
    cache_top_page_comments

    # for Users
    cache_users

    # for Products
    cache_products

    # for user_buy_histories
    cache_user_buy_histories

    pp dalli.get("user_1_buy_histories")
    pp dalli.get("user_5000_buy_histories")

    "Finish"
  end
end
