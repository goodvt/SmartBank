import importlib

import View.DashboardScreen.dashboard_screen

# We have to manually reload the view module in order to apply the
# changes made to the code on a subsequent hot reload.
# If you no longer need a hot reload, you can delete this instruction.
importlib.reload(View.DashboardScreen.dashboard_screen)



 
class DashboardScreenController:
    """
    The `SampleScreenController` class represents a controller implementation.
    Coordinates work of the view with the model.
    The controller implements the strategy pattern. The controller connects to
    the view to control its actions.
    """

    def __init__(self, model):
        self.model = model  # Model.sample_screen.SampleScreenModel
        self.view = View.DashboardScreen.dashboard_screen.DashboardScreenView(controller=self, model=self.model)

    def get_view(self) -> View.DashboardScreen.dashboard_screen:
        return self.view

   