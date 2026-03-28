use leptos::prelude::*;
use leptos_use::storage::use_local_storage;
use codee::string::JsonSerdeCodec;
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Video{
    pub url: String
}
impl Video {
    pub fn new(url: String) -> Video {
        return Video{
            url: url
        };
    }
}
#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct MyState {
    pub items: Vec<Video>
}

fn add_video(url: String, set_state: WriteSignal<MyState>) {
    if url.len() > 0 {
        let video = Video::new(url.clone());
        set_state.update(|list: &mut MyState| list.items.push(video));
    }
}

// https://book.leptos.dev/view/01_basic_component.html
#[component]
fn App() -> impl IntoView {
    // Adding signal to track the text content of the input box
    let (text_value, set_text_value) = signal("".to_string());

    // Load values from the local storage key "my-state"
    let (state, set_state, _) = use_local_storage::<MyState, JsonSerdeCodec>("my-state");

    // Render the view
    return view! {
        <h1>Telegram Download Queue Manager</h1>
        <ul>
            {// We need a function here to force it to re-render on every call.
            move ||
                state.get()
                    .items
                    .into_iter()
                    .map(|video: Video| view!{<li> {video.url} </li>})
                    .collect_view()
            }
        </ul>
        <p>"Items in queue: " {move || state.get().items.len()}</p>
        <form style="display: flex; gap: 1rem"
            on:submit=move |ev| {
                // Preventing the default browser submit (with page reload)
                ev.prevent_default();

                let value = text_value.get();
                add_video(value, set_state);

                // Set new value (independent of old value)
                // If you want to have the new value be depending on the old one use `update`. See line 25
                set_text_value.set(String::new());
            }
        >
            <input 
                name="download_url" 
                on:input:target=move |ev| set_text_value.set(ev.target().value())
                prop:value={text_value}
                required
            />
            <button type="submit">
                // For some reason we need a "string" here the docs also can't really explain it
                // For rendering a rust value use: {value}
                "Download"
            </button>
        </form>
    }
}

fn main() {
    // Adding a console hook to show rust stack trace in browser
    console_error_panic_hook::set_once();

    // Mounting the root component of the app.
    leptos::mount::mount_to_body(App)
}
