# The screen's dictionary contains the objects of the models and controllers
# of the screens of the application.

from Model.sample_screen import SampleScreenModel
from Controller.sample_screen import SampleScreenController
from Model.dasboard_screen import DashboardScreenModel
from Controller.dashboard_screen import DashboardScreenController
from Model.operation_list_screen import OperationListScreenModel
from Controller.operation_list_screen import OperationListScreenController
from Model.driving_screen import DrivingScreenModel
from Controller.driving_screen import DrivingScreenController

screens = {
    'dashboard screen': {
        'model': DashboardScreenModel,
        'controller': DashboardScreenController,
    },
    'driving screen': {
        'model': DrivingScreenModel,
        'controller': DrivingScreenController,
    },
    'sample screen': {
        'model': SampleScreenModel,
        'controller': SampleScreenController,
    },
}