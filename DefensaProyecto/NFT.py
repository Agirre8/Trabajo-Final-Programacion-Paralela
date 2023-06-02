# Importar librerías necesarias
import bcrypt
import datetime

# Clase para representar un NFT
class NFT:
    def __init__(self, id, title, description, price, seller_id):
        self.id = id
        self.title = title
        self.description = description
        self.price = price
        self.seller_id = seller_id

# Clase para representar un usuario
class User:
    def __init__(self, id, username, password):
        self.id = id
        self.username = username
        self.password = self._hash_password(password)
        
    def _hash_password(self, password):
        salt = bcrypt.gensalt()
        hashed_password = bcrypt.hashpw(password.encode(), salt)
        return hashed_password
    
    def check_password(self, password):
        return bcrypt.checkpw(password.encode(), self.password)
    
# Clase para representar el servidor de gestión de NFTs
class NFTServer:
    def __init__(self):
        self.users = []
        self.nfts = []
        
    def register_user(self, username, password):
        user_id = len(self.users) + 1
        user = User(user_id, username, password)
        self.users.append(user)
        
    def login_user(self, username, password):
        for user in self.users:
            if user.username == username and user.check_password(password):
                return user
        return None
    
    def list_nfts(self):
        return self.nfts
    
    def create_nft(self, title, description, price, seller_id):
        nft_id = len(self.nfts) + 1
        nft = NFT(nft_id, title, description, price, seller_id)
        self.nfts.append(nft)
        
    def buy_nft(self, nft_id, buyer_id):
        nft = self.get_nft_by_id(nft_id)
        if nft and nft.seller_id != buyer_id:
            # Realizar lógica de compra, actualizar base de datos, etc.
            self.nfts.remove(nft)
            return True
        return False
    
    def get_nft_by_id(self, nft_id):
        for nft in self.nfts:
            if nft.id == nft_id:
                return nft
        return None

# Ejemplo de uso
server = NFTServer()

# Registro de usuarios
server.register_user("user1", "password1")
server.register_user("user2", "password2")

# Inicio de sesión de usuarios
user1 = server.login_user("user1", "password1")
user2 = server.login_user("user2", "password2")

# Creación de NFTs
server.create_nft("NFT1", "Descripción del NFT 1", 100, user1.id)
server.create_nft("NFT2", "Descripción del NFT 2", 200, user1.id)
server.create_nft("NFT3", "Descripción del NFT 3", 150, user2.id)

# Listado de NFTs
nfts = server.list_nfts()
for nft in nfts:
    print(f"ID: {nft.id}, Title: {nft.title}, Description: {nft.description}, Price: {nft.price}, Seller ID: {nft.seller_id}")

# Compra de un NFT
nft_id = 1
buyer_id = user2
