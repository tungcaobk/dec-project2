class Product:
    def __init__(self, id: int, product_id: str, description: str, status: str, history: str, duration: int, error: str):
        self.id = id
        self.product_id = product_id
        self.description = description
        self.status = status
        self.history = history
        self.duration = duration
        self.error = error

    @classmethod
    def from_product_id(cls, product_id: str):
        return cls(id=None, product_id=product_id, description=None, status=None, history=None, duration=None, error=None)

    def __str__(self):
        return (f"Product(id={self.id}, product_id='{self.product_id}', description='{self.description}', "
                f"status='{self.status}', history='{self.history}', duration='{self.duration}', error='{self.error}')")

