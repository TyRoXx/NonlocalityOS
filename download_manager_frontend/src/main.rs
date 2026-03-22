mod components;
use components::App;

fn main() {
    // Adding a console hook to show rust stack trace in browser
    console_error_panic_hook::set_once();

    // Mounting the root component of the app.
    leptos::mount::mount_to_body(App)
}
