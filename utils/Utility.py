from datetime import datetime

class Utility:
    def calculate_age(self,birth_date):
        today = datetime.now()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

    # Helper function to calculate days since last consulted
    def days_since_last_consulted(self,last_consulted_date):
        today = datetime.now()
        delta = today - last_consulted_date
        return delta.days