import strawberry
from typing import List
import requests
from strawberry.fastapi import GraphQLRouter
from fastapi import FastAPI
from uvicorn import run

@strawberry.type
class Success:
    success: bool

@strawberry.type
class Category:
    category_id: str
    name: str

@strawberry.type
class Product:
    product_id: str
    name: str
    desc: str
    price: float
    category_id: str
    quantity: int
    @strawberry.field
    def category(self) -> Category:
        return GetCategory(self.category_id)

@strawberry.type
class ProductInCart:
    cart_product_id: str
    quantity: int
    product_id: str
    @strawberry.field
    def product(self) -> Product:
        return GetProduct(self.product_id)

@strawberry.type
class User:
    uid: str
    first_name: str
    second_name: str
    email: str
    adress: str
    is_admin: bool
    balance: float
    @strawberry.field
    def cart(self) -> List[ProductInCart]:
        return GetCart(self.uid)

@strawberry.type
class Query:
    @strawberry.field
    def users(self) -> List[User]:
        return GetUsers()
    @strawberry.field
    def user(self, uid: str) -> User:
        return GetUser(uid)


def GetUsers() -> List[User]:
    r = requests.post('http://gateway:8080/api/auth/getusers', json={})
    users = []
    for user in r.json()['users']:
        users.append(User(
            uid = user['uid'],
            first_name = user['first_name'],
            second_name = user['second_name'],
            email = user['email'],
            adress = user['adress'],
            is_admin = user['is_admin'],
            balance = user['balance']
        ))
    return users

def GetUser(uid: str) -> User:
    r = requests.post('http://gateway:8080/api/auth/getuser', json={"uid": uid})
    user = r.json()
    return User(
        uid=user['uid'],
        first_name=user['first_name'],
        second_name=user['second_name'],
        email=user['email'],
        adress=user['adress'],
        is_admin=user['is_admin'],
        balance=user['balance']
    )

def GetCart(uid: str) -> List[ProductInCart]:
    r = requests.post('http://gateway:8080/api/order/getcart', json={'uid': uid})
    cart = []
    for product_in_cart in r.json()['products']:
        cart.append(ProductInCart(
            cart_product_id = product_in_cart['cart_product_id'],
            quantity = product_in_cart['quantity'],
            product_id = product_in_cart['product_id']
        ))
    return cart

def GetProduct(product_id) -> Product:
    r = requests.post('http://gateway:8080/api/catalog/getproduct', json={"product_id": product_id})
    product = r.json()
    return Product(
        product_id = product['product_id'],
        name = product['name'],
        desc = product['desc'],
        price = product['price'],
        category_id = product['category_id'],
        quantity = product['quantity']
    )

def GetCategory(category_id) -> Category:
    r = requests.post('http://gateway:8080/api/catalog/getcategory', json={"category_id": category_id})
    category = r.json()
    return Category(
        category_id=category['category_id'],
        name=category['name']
    )

@strawberry.type
class Mutation:
    @strawberry.mutation
    def sign_up(
        self,
        first_name: str,
        second_name: str,
        email: str,
        password: str,
        adress: str,
        is_admin: bool
    ) -> User:
        json = {
            'first_name': first_name,
            'second_name': second_name,
            'email': email,
            'password': password,
            'adress': adress,
            'is_admin': is_admin
        }
        r = requests.post('http://gateway:8080/api/auth/signup', json=json)
        user = r.json()
        return GetUser(user['uid'])
    
    @strawberry.mutation
    def create_category(self, name: str) -> Category:
        r = requests.post('http://gateway:8080/api/catalog/createcategory', json={"name": name})
        category = r.json()
        return Category(
            category_id=category['category_id'],
            name=category['name']
        )
    
    @strawberry.mutation
    def create_product(
        self,
        name: str,
        desc: str,
        price: float,
        category_id: str,
        quantity: int
    ) -> Product:
        json = {
            'name': name,
            'desc': desc,
            'price': price,
            'category_id': category_id,
            'quantity': quantity
        }
        r = requests.post('http://gateway:8080/api/catalog/createproduct', json=json)
        product = r.json()
        return Product(
            product_id=product['product_id'],
            name=product['name'],
            desc=product['desc'],
            price=product['price'],
            category_id=product['category_id'],
            quantity=product['quantity']
        )

    @strawberry.mutation
    def add_to_cart(
            self,
            uid: str,
            product_id: str,
            quantity: int
    ) -> Success:
        json = {
            'uid': uid,
            'product_id': product_id,
            'quantity': quantity
        }
        r = requests.post('http://gateway:8080/api/order/addtocart', json=json)
        success = r.json()
        return Success(success=success['success'])

schema = strawberry.Schema(query=Query, mutation=Mutation)
graphql_app = GraphQLRouter(schema)

app = FastAPI()
app.include_router(graphql_app, prefix='/graphql')

if __name__ == '__main__':
    run(app, host='0.0.0.0', port=8000)

#users{
#    uid,
#    first_name,
#    second_name,
#    email,
#    adress,
#    is_admin,
#    balance,
#    cart{
#        product{
#            product_id,
#            name,
#            desc,
#            price,
#            category{
#                category_id,
#                name
#            },
#            quantity
#        }
#    }
#}