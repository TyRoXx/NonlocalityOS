use codee::string::JsonSerdeCodec;
use leptos::prelude::*;
use leptos::web_sys::{FormData, HtmlFormElement, SubmitEvent};
use leptos_use::storage::use_local_storage;
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsCast;

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Video {
    pub id: usize,
    pub name: String,
    pub url: String,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct MyState {
    pub items: Vec<Video>,
    pub currently_downloading: Option<Video>,
}

fn add_video(id: usize, name: String, url: String, set_state: WriteSignal<MyState>) {
    let clean_url = url.trim();
    if !clean_url.is_empty() {
        let video = Video {
            id,
            name,
            url: clean_url.to_string(),
        };
        set_state.update(|list: &mut MyState| {
            if list.items.len() == 0 {
                list.currently_downloading = Some(video.clone());
            }
            list.items.push(video)
        });
    }
}

fn submit_download(ev: SubmitEvent, state: Signal<MyState>, set_state: WriteSignal<MyState>) {
    ev.prevent_default();

    // Get the form element
    let Some(form) = ev
        .target()
        .and_then(|t| t.dyn_into::<HtmlFormElement>().ok())
    else {
        return;
    };

    // Get the form data
    let Ok(data) = FormData::new_with_form(&form) else {
        return;
    };

    let name = data.get("download_name").as_string().unwrap_or_default();
    let url = data.get("download_url").as_string().unwrap_or_default();

    add_video(state.get().items.len(), name, url, set_state);
    form.reset();
}

// https://book.leptos.dev/view/01_basic_component.html
#[component]
pub fn App() -> impl IntoView {
    // Load values from the local storage key "my-state"
    let (state, set_state, _) = use_local_storage::<MyState, JsonSerdeCodec>("my-state");

    // Render the view
    return view! {
        <main class="flex flex-col gap-2 p-4">
        <h1 class="text-2xl font-bold">Telegram Download Queue Manager</h1>
            <div>
                <h2 class="text-lg">"Currently downloading"</h2>
                <div class="flex flex-row gap-2">
                    <img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AEj6PgMOAYFTNFy/AAAAAElFTkSuQmCC" 
                        alt="Video thumbnail" 
                        class="w-20 h-20 aspect-square" />
                    <div class="flex flex-col gap-2 grow">
                        {move || match state.get().currently_downloading {
                            None => view! {
                                "No video currently downloading"
                            }
                            .into_any(),
                            Some(v) => view! {
                                <p class="text-sm">{v.name}</p>
                                <p class="text-xs text-stone-500">{v.url}</p>
                                <meter value="50" max="100" />
                            }
                            .into_any(),
                        }}
                    </div>
                </div>
            </div>

            <div>
                <h2 class="text-lg">"Next up in queue (" {move || state.get().items.len()} ")"</h2>
                <ul class="list-disc list-inside">
                    <For
                        // How to get the list of items to iterate over
                        each=move || state.get().items
                        // Generate a key (like an id for the dom for each element
                        key=move |video| video.id
                        // How to render a child
                        children=move |video: Video| view!{
                            <li> {video.name} <small class="pl-1 text-stone-500">{video.url}</small> </li>
                        }
                    />
                </ul>
            </div>
        
            <div>
                <form class="flex gap-2"
                    on:submit=move |ev: SubmitEvent| {
                        submit_download(ev, state, set_state);
                    }
                >
                    <input
                        name="download_name"
                        placeholder="Video Name"
                        required
                        class="border border-gray-300 rounded-md p-2"
                    />
                    <input
                        name="download_url"
                        placeholder="Video URL"
                        required
                        class="border border-gray-300 rounded-md p-2"
                    />
                    <button type="submit" class="bg-blue-500 text-white px-4 py-2 rounded-md">
                        // For some reason we need a "string" here the docs also can't really explain it
                        // For rendering a rust value use: {value}
                        "Download"
                    </button>
                </form>
            </div>
        </main>
    };
}
