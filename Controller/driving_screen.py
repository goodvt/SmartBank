import importlib

import View.DrivingScreen.driving_screen
from kivymd.uix.pickers import MDDockedDatePicker
from datetime import datetime
from kivy.metrics import dp


# We have to manually reload the view module in order to apply the
# changes made to the code on a subsequent hot reload.
# If you no longer need a hot reload, you can delete this instruction.
importlib.reload(View.DrivingScreen.driving_screen)




class DrivingScreenController:
    """
    The `DrivingScreenController` class represents a controller implementation.
    Coordinates work of the view with the model.
    The controller implements the strategy pattern. The controller connects to
    the view to control its actions.
    """

    

    def __init__(self, model):
        self.model = model  # Model.driving_screen.DrivingScreenModel
        self.view = View.DrivingScreen.driving_screen.DrivingScreenView(controller=self, model=self.model)
        #self.date_imputation = StringProperty('')

    def get_view(self) -> View.DrivingScreen.driving_screen:
        return self.view

    def show_date_picker(self, instance_of_textfield, is_focused):
        if not is_focused:
            return

        date_dialog = MDDockedDatePicker()               
        # You have to control the position of the date picker dialog yourself.
        date_dialog.pos = [
            (self.view.width - date_dialog.width) / 2,
            (self.view.height - date_dialog.height)/2 ,
        ]
    
        #Lien entre la méthode et la selection de la date
        date_dialog.bind(on_select_day=self.on_select_day)
        #date_dialog.bind(on_ok=self.controller.on_ok)
        date_dialog.open()


    def on_select_day(self, instance_date_picker, number_day):
        instance_date_picker.dismiss()
        self.view.date_imputation = instance_date_picker.get_date()[0].strftime("%Y-%m-%d")        
    
    def save_data(self):
        pass