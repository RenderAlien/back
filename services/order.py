from concurrent import futures
import grpc

import order_pb2, order_pb2_grpc
import auth_pb2, auth_pb2_grpc
import catalog_pb2, catalog_pb2_grpc

import logging
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('OrderService')

class OrderService(order_pb2_grpc.OrderServicer):

    def __init__(self):
        logger.info('Initializing Order Service...')

        self.carts = {}
        # carts[uid] = {cart_product_id: {product_id, quantity}  }

        self.catalog_channel = grpc.insecure_channel('catalog:50052')
        self.catalog_stub = catalog_pb2_grpc.CatalogStub(self.catalog_channel)
        self.auth_channel = grpc.insecure_channel('auth:50051')
        self.auth_stub = auth_pb2_grpc.AuthStub(self.auth_channel)
        
        
        logger.info("Order Service initialized successfully")
    
    def GetCart(self, request, context):
        logger.info('Getting Cart...')
        if request.uid not in self.carts:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
            if not uid:
                logger.warning("This user doesn't exist.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("This user doesn't exist.")
                return order_pb2.GetCartResponse()
            self.carts[uid] = {}

        return order_pb2.GetCartResponse(
            products = [order_pb2.Product(cart_product_id=cart_product_id, **self.carts[request.uid][cart_product_id]) for cart_product_id in self.carts[request.uid]]
        )
    
    def GetFromCart(self, request, context):
        logger.info('Getting From Cart...')
        if request.uid not in self.carts:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
            if not uid:
                logger.warning("This user doesn't exist.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("This user doesn't exist.")
                return order_pb2.Product()
            self.carts[uid] = {}
        
        if request.cart_product_id not in self.carts[uid]:
            logger.warning("This product doesn't exist in cart.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("This product doesn't exist in cart.")
            return order_pb2.Product()
        
        return order_pb2.Product(cart_product_id=request.cart_product_id, **self.carts[uid][request.cart_product_id])
    
    def AddToCart(self, request, context):
        logger.info('Adding To Cart...')
        if request.uid not in self.carts:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
            if not uid:
                logger.warning("This user doesn't exist.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("This user doesn't exist.")
                return order_pb2.SuccessResponse(success=False)
            self.carts[uid] = {}

        try:
            catalog_resp = self.catalog_stub.GetProduct(catalog_pb2.GetProductRequest(
                product_id = request.product_id
            ))
            product_id, quantity = catalog_resp.product_id, catalog_resp.quantity
        except Exception as e:
            product_id = ''
            quantity = 0
        if not product_id:
            logger.warning("This product doesn't exist.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("This product doesn't exist.")
            return order_pb2.SuccessResponse(success=False)
        if request.quantity > quantity:
            logger.warning("Quantity of this product is out of range.")
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            context.set_details("Quantity of this product is out of range.")
            return order_pb2.SuccessResponse(success=False)
        
        cart_product_id = str(uuid.uuid4())
        self.carts[request.uid][cart_product_id] = {'product_id': request.product_id, 'quantity': request.quantity}
        return order_pb2.SuccessResponse(success=True)
    
    def DeleteFromCart(self, request, context):
        logger.info('Deleting From Cart...')
        if request.uid not in self.carts:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
            if not uid:
                logger.warning("This user doesn't exist.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("This user doesn't exist.")
                return order_pb2.SuccessResponse(success=False)
            self.carts[uid] = {}
        if request.cart_product_id not in self.carts[request.uid]:
            logger.warning("This product doesn't exist in this cart.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("This product doesn't exist in this cart.")
            return order_pb2.SuccessResponse(success=False)
        del self.carts[request.uid][request.cart_product_id]
        return order_pb2.SuccessResponse(success=True)
    
    def UpdateWithinCart(self, request, context):
        logger.info('Updating Within Cart...')
        if request.uid not in self.carts:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
            if not uid:
                logger.warning("This user doesn't exist.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("This user doesn't exist.")
                return order_pb2.SuccessResponse(success=False)
            self.carts[uid] = {}

        if request.cart_product_id not in self.carts[request.uid]:
            logger.warning("This product doesn't exist in this cart.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("This product doesn't exist in this cart.")
            return order_pb2.SuccessResponse(success=False)
        
        try:
            catalog_resp = self.catalog_stub.GetProduct(catalog_pb2.GetProductRequest(
                product_id = self.carts[request.uid][request.cart_product_id]['product_id']
            ))
            quantity = catalog_resp.quantity
        except Exception as e:
            logger.warning("Updating Within Cart Failed.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Updating Within Cart Failed.")
            return order_pb2.SuccessResponse(success=False)
        
        if request.quantity > quantity:
            logger.warning("Quantity of this product is out of range.")
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            context.set_details("Quantity of this product is out of range.")
            return order_pb2.SuccessResponse(success=False)
        
        self.carts[request.uid][request.cart_product_id]['quantity'] = request.quantity
        return order_pb2.SuccessResponse(success=True)
    
    def BuyFromCart(self, request, context):
        logger.info('Updating Within Cart...')
        if request.uid not in self.carts:
            try:
                uid = self.auth_stub.GetUser(auth_pb2.GetUserRequest(
                    uid = request.uid
                )).uid
            except Exception as e:
                uid = ''
            if not uid:
                logger.warning("This user doesn't exist.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("This user doesn't exist.")
                return order_pb2.SuccessResponse(success=False)
            self.carts[uid] = {}

        if request.cart_product_id not in self.carts[request.uid]:
            logger.warning("This product doesn't exist in this cart.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("This product doesn't exist in this cart.")
            return order_pb2.SuccessResponse(success=False)
        
        try:
            catalog_resp = self.catalog_stub.GetProduct(catalog_pb2.GetProductRequest(
                product_id = self.carts[request.uid][request.cart_product_id]['product_id']
            ))
            quantity = catalog_resp.quantity
            price = catalog_resp.price
        except Exception as e:
            logger.warning("Pulling product from Catalog failed.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Pulling product from Catalog failed.")
            return order_pb2.SuccessResponse(success=False)
        
        if self.carts[request.uid][request.cart_product_id]['quantity'] > quantity:
            logger.warning("Quantity of this product is out of range.")
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            context.set_details("Quantity of this product is out of range.")
            return order_pb2.SuccessResponse(success=False)
        
        try:
            user_balance = self.auth_stub.GetUser(auth_pb2.GetUserRequest(uid=request.uid)).balance
        except Exception as e:
            logger.warning("Pulling user balance from Auth failed.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Pulling user balance from Auth failed.")
            return order_pb2.SuccessResponse(success=False)
        
        if user_balance < self.carts[request.uid][request.cart_product_id]['quantity'] * price:
            logger.warning("Not enough money.")
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            context.set_details("Not enough money.")
            return order_pb2.SuccessResponse(success=False)
        
        try:
            success = self.auth_stub.UpdateUserBalance(auth_pb2.UpdateUserBalanceRequest(
                uid=request.uid,
                balance = user_balance - self.carts[request.uid][request.cart_product_id]['quantity'] * price
            )).success
        except Exception as e:
            logger.warning("Updating User Balance failed.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Updating User Balance failed.")
            return order_pb2.SuccessResponse(success=False)
        
        if not success:
            logger.warning("Buying From Cart Failed.")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Buying From Cart Failed.")
            return order_pb2.SuccessResponse(success=False)
        
        del self.carts[request.uid][request.cart_product_id]
        return order_pb2.SuccessResponse(success=True)
        


def serve():
    logger.info('Starting Order Service...')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_pb2_grpc.add_OrderServicer_to_server(OrderService(), server)
    server.add_insecure_port("[::]:50053")
    logger.info('Order Service successfully started on port 50053.')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    try:
        serve()
    except KeyboardInterrupt:
        logger.info('Order Service stopped by user.')
    except Exception as e:
        logger.error(f'Order Service crashed: {str(e)}')