from View.DashboardScreen.dashboard_screen import DashboardScreenView
from View.OperationScreen.operation_screen import OperationScreenView

# The screens dictionary contains the objects of the models and controllers
# of the screens of the application.


from Model.sample_screen import SampleScreenModel
from Model.dasboard_screen import DashboardScreenModel
from Controller.sample_screen import SampleScreenController
from Controller.dashboard_screen import DashboardScreenController

screens = {
        'dashboard screen': {
        "model": DashboardScreenModel,
        "controller": DashboardScreenController,
    },
    "sample screen": {
        "model": SampleScreenModel,
        "controller": SampleScreenController,
    }
}