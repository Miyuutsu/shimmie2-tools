    for batch in tqdm.tqdm(batches, desc="Tagging images"):

        # Preprocess and store md5s and images
        # === Multi-threaded post resolution ===
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            results = list(tqdm.tqdm(
                executor.map(lambda img: resolve_post(img, cache_path), batch),
                total=len(batch),
                desc="Resolving posts"
            ))

        # Get images that need tagging
        tag_needed = [img for img, post in results if post is None]
        img_inputs = process_batch(tag_needed, transform)

        if img_inputs is not None and len(img_inputs) > 0:
            with torch.inference_mode():
                batched_tensor = img_inputs.to(torch_device)
                raw_outputs = F.sigmoid(model(batched_tensor)).cpu()
            raw_outputs = list(torch.unbind(raw_outputs, dim=0))
        else:
            raw_outputs = []

        out_idx = 0
        for image, post in results:
            # If post is still missing, run the tagger
            if not post:
                if out_idx >= len(img_inputs):
                    print(f"[WARN] Out-of-bounds image tensor for: {image.name}")
                    continue
                img_tensor = img_inputs[out_idx]
                if img_tensor is None:
                    continue  # skip if failed to load
                probs = raw_outputs[out_idx]
                out_idx += 1

                char, gen, artist, series, rating = get_tags(
                    probs,
                    labels,
                    args.gt,
                    args.ct,
                    args.rt
                )

                post = {
                    "character": [t[0] for t in char.items()],
                    "general": [t[0] for t in gen.items()],
                    "artist": [t[0] for t in artist.items()],
                    "series": [t[0] for t in series.items()],
                    "rating": [t[0] for t in rating.items()],
                    "source": None
                }

                character_tags = [t[0] for t in char.items()]
                general_tags = [t[0] for t in gen.items()]
                rating_tags = [t[0] for t in rating.items()]
                series_tags = set()

                for char_tag in character_tags:
                    if char_tag in character_series_map:
                        inferred_series = character_series_map[char_tag]
                        series_tags.add(inferred_series)

                # ✅ Append inferred series to post
                post["series"].extend(sorted(series_tags))

                if file_path.is_file(db_dir / "tag_rating_dominant.db"):
                    rating_priority = {'e': 3, 'q': 2, 's': 1}
                    tag_db_path = db_dir / "tag_rating_dominant.db"
                    tag_db_conn = sqlite3.connect(tag_db_path)
                    tag_db_cursor = tag_db_conn.cursor()
                    tag_db_cursor.execute(f"SELECT * FROM dominant_tag_ratings")
                    rows = cursor.fetchall()
                    rating_letter = None

                    for row in rows:
                        if len(row) >= 2:
                            db_tag, db_rating = row[0].strip(), row[1].strip()
                            for gen_tag in general_tags:
                                if gen_tag in db_tag:
                                    if db_rating == 'e' and rating_letter != 'e':
                                        rating_letter = 'e'
                                        break  # Stop further checks, as 'e' is the highest priority
                                    elif db_rating == 'q' and rating_letter not in ['e', 'q']:
                                        rating_letter = 'q'
                                        break  # Stop further checks if 'q' is found and no 'e'
                                    elif db_rating == 's' and rating_letter not in ['e', 'q', 's']:
                                        rating_letter = 's'
                                        break  # Stop further checks if 's' is found and no higher priority
                    if rating_letter:
                        break

                    else:
                        if "explicit" in rating:
                            rating_letter = "e"
                        elif "questionable" in rating or "sensitive" in rating:
                            rating_letter = "q"
                        elif "general" in rating:
                            rating_letter = "s"
                        else:
                            rating_letter = "?"

            # Build tags from post
            tags = []
            tags.extend(post.get("general", []))
            tags.extend(f"character:{t}" for t in post.get("character", []))
            tags.extend(f"series:{t}" for t in post.get("series", []))
            tags.extend(f"artist:{t}" for t in post.get("artist", []))

            # Clean rating-related tags and append a single normalized one
            tags = [t for t in tags if t not in ("general", "sensitive", "questionable", "explicit")]
            tags = [t for t in tags if not t.startswith("rating=")]

            rating_letter = post.get("rating", "?")
            tags.append(f"rating={rating_letter}")

            if post.get("source"):
                tags.append(f"source:{post['source']}")

            tags = sorted(set(tags))
            tag_str = ", ".join(tags)

            rel_path = image.relative_to(image_path)
            csv_rows.append([
                f"import/{rel_path}",
                tag_str,
                "",
                rating_letter,
                ""
            ])
    print(f"[✓] Ran tagger on {out_idx} of {len(images)} images.")
