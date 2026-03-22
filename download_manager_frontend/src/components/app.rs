use codee::string::JsonSerdeCodec;
use leptos::prelude::*;
use leptos::web_sys::{FormData, HtmlFormElement, SubmitEvent};
use leptos_use::storage::use_local_storage;
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsCast;

/// 1×1 red PNG (idle / no download).
const THUMB_RED_PIXEL: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";
/// 1×1 gray PNG (download in progress).
const THUMB_GRAY_PIXEL: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AEj6PgMOAYFTNFy/AAAAAElFTkSuQmCC";

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Video {
    pub id: usize,
    pub name: String,
    pub url: String,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct DownloadManagerData {
    pub items: Vec<Video>,
    pub currently_downloading: Option<Video>,
}

fn add_video(id: usize, name: String, url: String, set_state: WriteSignal<DownloadManagerData>) {
    let clean_url = url.trim();
    if !clean_url.is_empty() {
        let video = Video {
            id,
            name,
            url: clean_url.to_string(),
        };
        set_state.update(|list: &mut DownloadManagerData| {
            if list.items.is_empty() {
                list.currently_downloading = Some(video.clone());
            }
            list.items.push(video)
        });
    }
}

fn remove_video(id: usize, set_state: WriteSignal<DownloadManagerData>) {
    set_state.update(|s: &mut DownloadManagerData| {
        let removed_current = s.currently_downloading.as_ref().is_some_and(|c| c.id == id);
        s.items.retain(|v| v.id != id);
        if removed_current {
            s.currently_downloading = s.items.first().cloned();
        }
    });
}

fn submit_download(
    ev: SubmitEvent,
    state: Signal<DownloadManagerData>,
    set_state: WriteSignal<DownloadManagerData>,
) {
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

    let next_id = state.with(|s| {
        s.items
            .iter()
            .map(|v| v.id)
            .max()
            .map(|m| m + 1)
            .unwrap_or(0)
    });
    add_video(next_id, name, url, set_state);
    form.reset();
}

// https://book.leptos.dev/view/01_basic_component.html
#[component]
pub fn App() -> impl IntoView {
    // Load values from the local storage key "download-manager:data"
    let (download_manager_data, set_download_manager_data, _) =
        use_local_storage::<DownloadManagerData, JsonSerdeCodec>("download-manager:data");

    // Render the view
    view! {
        <main class="flex flex-col gap-2 p-4">
        <h1 class="text-2xl font-bold">Telegram Download Queue Manager</h1>
            <div>
                <h2 class="text-lg">"Currently downloading"</h2>
                <div class="flex flex-row gap-2">
                    <img
                        src=move || match download_manager_data.get().currently_downloading {
                            None => THUMB_RED_PIXEL,
                            Some(_) => THUMB_GRAY_PIXEL,
                        }
                        alt="Video thumbnail"
                        class="w-20 h-20 aspect-square"
                    />
                    <div class="flex flex-col gap-2 grow">
                        {move || match download_manager_data.get().currently_downloading {
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
                <h2 class="text-lg">"Next up in queue (" {move || download_manager_data.get().items.len()} ")"</h2>
                <ul class="flex flex-col gap-1">
                    <For
                        // How to get the list of items to iterate over
                        each=move || download_manager_data.get().items
                        // Generate a key (like an id for the dom for each element
                        key=move |video| video.id
                        // How to render a child
                        children=move |video: Video| {
                            let id = video.id;
                            view! {
                                <li class="flex flex-row gap-2 items-center">
                                    <button
                                        type="button"
                                        class="text-sm bg-stone-200 hover:bg-stone-300 p-1 rounded-md"
                                        on:click=move |_| remove_video(id, set_download_manager_data)
                                        attr:aria-label="Remove video from queue"
                                    >
                                        "❌"
                                    </button>
                                    <span class="grow">
                                        {video.name}
                                        <small class="pl-1 text-stone-500">{video.url}</small>
                                    </span>

                                </li>
                            }
                        }
                    />
                </ul>
            </div>

            <div>
                <form class="flex gap-2"
                    on:submit=move |ev: SubmitEvent| {
                        submit_download(ev, download_manager_data, set_download_manager_data);
                    }
                >
                    <label>
                        Name:
                        <input
                            name="download_name"
                            placeholder="Video Name"
                            required
                            class="border border-gray-300 rounded-md p-2"
                        />
                    </label>
                    <label>
                        Download URL
                        <input
                            name="download_url"
                            placeholder="Video URL"
                            required
                            class="border border-gray-300 rounded-md p-2"
                        />
                    </label>
                    <button type="submit" class="bg-blue-500 text-white px-4 py-2 rounded-md">
                        // In `view!`, string literals are rendered as text nodes.
                        // Use `{value}` when inserting a Rust expression dynamically.
                        "Download"
                    </button>
                </form>
            </div>
        </main>
    }
}
