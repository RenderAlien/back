from flask import Flask, request, jsonify
import grpc
import auth_pb2
import auth_pb2_grpc
import catalog_pb2
import catalog_pb2_grpc

app = Flask(__name__)

SERVICE_CONFIG = {
    'auth': 'auth:50051',
    'catalog': 'catalog:50052'
}

class AuthClient:

    def __init__(self):
        self.channel = grpc.insecure_channel(SERVICE_CONFIG['auth'])
        self.stub = auth_pb2_grpc.AuthStub(self.channel)
    
    def SignUp(self, data):
        try:
            response = self.stub.SignUp(auth_pb2.SignUpRequest(
                first_name = data['first_name'],
                second_name = data['second_name'],
                email = data['email'],
                password = data['password'],
                adress = data['adress'],
                is_admin = data['is_admin']
            ))
            return {
                'token': response.token,
                'uid': response.uid,
                'is_admin': response.is_admin
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
        
    def SignIn(self, data):
        try:
            response = self.stub.SignIn(auth_pb2.SignInRequest(
                email = data['email'],
                password = data['password']
            ))
            return {
                'token': response.token,
                'uid': response.uid,
                'is_admin': response.is_admin
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def GetUser(self, data):
        try:
            response = self.stub.GetUser(auth_pb2.GetUserRequest(
                uid = data['uid']
            ))
            return {
                'uid': response.uid,
                'first_name': response.first_name,
                'second_name': response.second_name,
                'email': response.email,
                'adress': response.adress,
                'is_admin': response.is_admin
            }
        except grpc.RpcError as e:
            return {'error': e.details()}

class CatalogClient:

    def __init__(self):
        self.channel = grpc.insecure_channel(SERVICE_CONFIG['catalog'])
        self.stub = catalog_pb2_grpc.CatalogStub(self.channel)
    
    def GetAllProducts(self, data):
        try:
            response = self.stub.GetAllProducts(catalog_pb2.Empty())
            return {
                'products':[{'product_id': product.product_id, 'name': product.name, 'desc': product.desc, 'price': product.price, 'category_id': product.category_id, 'quantity': product.quantity} for product in response.products]
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
        
    def GetAllCategories(self, data):
        try:
            response = self.stub.GetAllCategories(catalog_pb2.Empty())
            return {
                'categories':[{'category_id': category.category_id, 'name': category.name} for category in response.categories]
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def SearchProducts(self, data):
        try:
            response = self.stub.SearchCategories(catalog_pb2.SearchRequest(
                search_request = data['search_request']
            ))
            return {
                'products':[{'product_id': product.product_id, 'name': product.name, 'desc': product.desc, 'price': product.price, 'category_id': product.category_id, 'quantity': product.quantity} for product in response.products]
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def CreateProduct(self, data):
        try:
            response = self.stub.CreateProduct(catalog_pb2.CreateProductRequest(
                name = data['name'],
                desc = data['desc'],
                price = data['price'],
                category_id = data['category_id'],
                quantity = data['quantity']
            ))
            return {
                'product_id': response.product_id,
                'name': response.name,
                'desc': response.desc,
                'price': response.price,
                'category_id': response.category_id,
                'quantity': response.quantity
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
        
    def CreateCategory(self, data):
        try:
            response = self.stub.CreateCategory(catalog_pb2.CreateCategoryRequest(
                name = data['name']
            ))
            return {
                'category_id': response.category_id,
                'name': response.name
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
        
    def UpdateProduct(self, data):
        try:
            response = self.stub.UpdateProduct(catalog_pb2.UpdateProductRequest(
                product_id = data['product_id'],
                name = data['name'],
                desc = data['desc'],
                price = data['price'],
                category_id = data['category_id'],
                quantity = data['quantity']
            ))
            return {
                'product_id': response.product_id,
                'name': response.name,
                'desc': response.desc,
                'price': response.price,
                'category_id': response.category_id,
                'quantity': response.quantity
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
        
    def UpdateCategory(self, data):
        try:
            response = self.stub.UpdateCategory(catalog_pb2.UpdateCategoryRequest(
                category_id = data['category_id'],
                name = data['name']
            ))
            return {
                'category_id': response.category_id,
                'name': response.name
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def GetProduct(self, data):
        try:
            response = self.stub.GetProduct(catalog_pb2.GetProductRequest(
                product_id = data['product_id']
            ))
            return {
                'product_id': response.product_id,
                'name': response.name,
                'desc': response.desc,
                'price': response.price,
                'category_id': response.category_id,
                'quantity': response.quantity
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def GetCategory(self, data):
        try:
            response = self.stub.GetCategory(catalog_pb2.GetCategoryRequest(
                category_id = data['category_id']
            ))
            return {
                'category_id': response.category_id,
                'name': response.name
            }
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def DeleteProduct(self, data):
        try:
            response = self.stub.DeleteProduct(catalog_pb2.DeleteProductRequest(
                product_id = data['product_id']
            ))
            return {'success': response.success}
        except grpc.RpcError as e:
            return {'error': e.details()}
    
    def DeleteCategory(self, data):
        try:
            response = self.stub.DeleteCategory(catalog_pb2.DeleteCategoryRequest(
                category_id = data['category_id']
            ))
            return {'success': response.success}
        except grpc.RpcError as e:
            return {'error': e.details()}

auth_client = AuthClient()
catalog_client = CatalogClient()

@app.route('/api/auth/signup', methods=['POST'])
def SignUp():
    data = request.get_json()
    result = auth_client.SignUp(data)
    return jsonify(result)

@app.route('/api/auth/signin', methods=['POST'])
def SignIn():
    data = request.get_json()
    result = auth_client.SignIn(data)
    return jsonify(result)

@app.route('/api/auth/getuser', methods=['POST'])
def GetUser():
    data = request.get_json()
    result = auth_client.GetUser(data)
    return jsonify(result)

@app.route('/api/catalog/getallproducts', methods=['POST'])
def GetAllProducts():
    data = request.get_json()
    result = catalog_client.GetAllProducts(data)
    return jsonify(result)

@app.route('/api/catalog/getallcategories', methods=['POST'])
def GetAllCategories():
    data = request.get_json()
    result = catalog_client.GetAllCategories(data)
    return jsonify(result)

@app.route('/api/catalog/searchproducts', methods=['POST'])
def SearchProducts():
    data = request.get_json()
    result = catalog_client.SearchProducts(data)
    return jsonify(result)

@app.route('/api/catalog/createproduct', methods=['POST'])
def CreateProduct():
    data = request.get_json()
    result = catalog_client.CreateProduct(data)
    return jsonify(result)

@app.route('/api/catalog/createcategory', methods=['POST'])
def CreateCategory():
    data = request.get_json()
    result = catalog_client.CreateCategory(data)
    return jsonify(result)

@app.route('/api/catalog/updateproduct', methods=['POST'])
def UpdateProduct():
    data = request.get_json()
    result = catalog_client.UpdateProduct(data)
    return jsonify(result)

@app.route('/api/catalog/updatecategory', methods=['POST'])
def UpdateCategory():
    data = request.get_json()
    result = catalog_client.UpdateCategory(data)
    return jsonify(result)

@app.route('/api/catalog/getproduct', methods=['POST'])
def GetProduct():
    data = request.get_json()
    result = catalog_client.GetProduct(data)
    return jsonify(result)

@app.route('/api/catalog/getcategory', methods=['POST'])
def GetCategory():
    data = request.get_json()
    result = catalog_client.GetCategory(data)
    return jsonify(result)

@app.route('/api/catalog/deleteproduct', methods=['POST'])
def DeleteProduct():
    data = request.get_json()
    result = catalog_client.DeleteProduct(data)
    return jsonify(result)

@app.route('/api/catalog/deletecategory', methods=['POST'])
def DeleteCategory():
    data = request.get_json()
    result = catalog_client.DeleteCategory(data)
    return jsonify(result)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)